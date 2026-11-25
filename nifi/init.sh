#!/bin/sh

set -e

# NIFI_AUTH selects how this script reaches the NiFi API (see nifi/README.md):
#   none:     plain HTTP, no authentication.
#   keycloak: HTTPS, authenticated with the machine identity certificate that
#             the NiFi container writes to the shared nifi_machine_identity
#             volume. This container must run as uid 1000 to read its key.
NIFI_AUTH_MODE=$(printf '%s' "${NIFI_AUTH:-none}" | tr '[:upper:]' '[:lower:]')
# Fixed path: docker-compose.yaml mounts the shared volume here in both
# containers. NIFI_TLS_CLIENT_DIR is deliberately not used, because NiFi would
# write the certificate to that path in its own container, off the volume.
MACHINE_IDENTITY_DIR="/opt/nifi/machine-identity"

case "$NIFI_AUTH_MODE" in
  none)
    BASE_URL="${NIFI_BASE_URL:-http://nifi:${NIFI_WEB_HTTP_PORT:-8088}/nifi-api}"
    ;;
  keycloak)
    BASE_URL="${NIFI_BASE_URL:-https://nifi:${NIFI_WEB_HTTPS_PORT:-8443}/nifi-api}"
    ;;
  *)
    echo "ERROR: unsupported NIFI_AUTH value '${NIFI_AUTH}' (expected none or keycloak)"
    exit 1
    ;;
esac

# Every NiFi API call goes through this wrapper so that it uses the right
# transport for the auth mode.
nifi_curl() {
  if [ "$NIFI_AUTH_MODE" = "keycloak" ]; then
    curl --cacert "$MACHINE_IDENTITY_DIR/ca.crt" \
      --cert "$MACHINE_IDENTITY_DIR/machine.crt" \
      --key "$MACHINE_IDENTITY_DIR/machine.key" \
      "$@"
  else
    curl "$@"
  fi
}

echo "NiFi auth mode: $NIFI_AUTH_MODE ($BASE_URL)"

PB_SENSITIVE_FALSE="${PB_SENSITIVE_FALSE:-${PB_SENSITIVE:-false}}"

# Core (five Tazama core DBs) and CMS (tazama_cms) are separate PostgreSQL
# instances, so each has its own host, port, user and password. Hosts and
# passwords have no default and are checked below.
CORE_PG_HOST="${CORE_PG_HOST:-}"
CORE_PG_PORT="${CORE_PG_PORT:-15432}"
CORE_PG_USER="${CORE_PG_USER:-postgres}"
CORE_PG_PASSWORD="${CORE_PG_PASSWORD:-}"
CMS_PG_HOST="${CMS_PG_HOST:-}"
CMS_PG_PORT="${CMS_PG_PORT:-15433}"
CMS_PG_USER="${CMS_PG_USER:-postgres}"
CMS_PG_PASSWORD="${CMS_PG_PASSWORD:-}"

# Fail fast on missing configuration, before anything is created in NiFi.
# An empty value would otherwise be stored as a parameter and only fail at
# runtime: DBCPConnectionPool does not connect when it is enabled, and the
# PostgreSQL driver replaces an empty host with localhost, so the services
# enable, the flow starts and the run is recorded as complete. A re-run after
# fixing .env would not repair it either, because existing parameters are not
# overwritten (see ensure_param below).
MISSING_VARS=""
for _var in PB_CONTEXT_NAME PB_NAME PB_BUCKET PB_HTTP_NAME PB_HTTP_VALUE \
  PB_OZONE_NAME PB_OZONE_ENDPOINT CORE_PG_HOST CORE_PG_PASSWORD \
  CMS_PG_HOST CMS_PG_PASSWORD S3A_ACCESS_KEY S3A_SECRET_KEY ORCHESTRATOR_API_KEY; do
  eval "_value=\${$_var:-}"
  if [ -z "$_value" ]; then
    MISSING_VARS="$MISSING_VARS $_var"
  fi
done
if [ -n "$MISSING_VARS" ]; then
  echo "ERROR: required variables are not set:$MISSING_VARS"
  echo "Set them in .env and run nifi-init again."
  exit 1
fi

echo "Waiting for NiFi API to be ready..."

# Wait until parameter-context API responds properly, with bounded retries.
WAIT_RETRIES="${NIFI_API_WAIT_RETRIES:-60}"
WAIT_DELAY_SECONDS="${NIFI_API_WAIT_DELAY_SECONDS:-5}"
WAIT_ATTEMPT=1

while [ "$WAIT_ATTEMPT" -le "$WAIT_RETRIES" ]; do
  if [ "$NIFI_AUTH_MODE" = "keycloak" ] && [ ! -r "$MACHINE_IDENTITY_DIR/machine.key" ]; then
    # NiFi writes the certificate at start-up, so it may not exist yet.
    echo "Machine identity key $MACHINE_IDENTITY_DIR/machine.key not readable yet (attempt $WAIT_ATTEMPT/$WAIT_RETRIES); it needs the nifi_machine_identity volume and uid 1000"
  elif nifi_curl -s "$BASE_URL/flow/parameter-contexts" | grep -q "parameterContexts"; then
    break
  else
    echo "NiFi API not ready yet (attempt $WAIT_ATTEMPT/$WAIT_RETRIES), retrying in ${WAIT_DELAY_SECONDS}s..."
  fi
  WAIT_ATTEMPT=$((WAIT_ATTEMPT + 1))
  sleep "$WAIT_DELAY_SECONDS"
done

if [ "$WAIT_ATTEMPT" -gt "$WAIT_RETRIES" ]; then
  echo "NiFi API did not become ready after $WAIT_RETRIES attempts"
  exit 1
fi

echo "NiFi API is ready"

# In keycloak mode the machine identity is NiFi's Initial Admin, but on a fresh
# install it only gets the root process group policies shortly after start-up
# (nifi/tazama-admin-group.sh). Wait until it can change the canvas.
ACCESS_ATTEMPT=1
while [ "$ACCESS_ATTEMPT" -le "$WAIT_RETRIES" ]; do
  if nifi_curl -s "$BASE_URL/flow/process-groups/root" \
    | tr -d '\n' \
    | grep -q '^{"permissions":{[^}]*"canWrite":true'; then
    break
  fi
  echo "Waiting for write access to the root process group (attempt $ACCESS_ATTEMPT/$WAIT_RETRIES)..."
  ACCESS_ATTEMPT=$((ACCESS_ATTEMPT + 1))
  sleep "$WAIT_DELAY_SECONDS"
done

if [ "$ACCESS_ATTEMPT" -gt "$WAIT_RETRIES" ]; then
  echo "ERROR: no write access to the root process group after $WAIT_RETRIES attempts"
  exit 1
fi

# Build the full parameter list (JSON fragments) from environment variables.
# Sensitive parameters (core/cms passwords, ozone keys) are stored encrypted by
# NiFi and are never returned by the REST API once set.
# Values are JSON-escaped with a sed pass because user-supplied secrets may
# contain quotes or backslashes, and the runtime image (curlimages/curl) does
# not ship jq.
json_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

PARAM_FRAGMENTS=""
add_param_fragment() {
  _name="$1"
  _value="$2"
  _sensitive="$3"
  _escaped_value=$(json_escape "$_value")
  if [ -n "$PARAM_FRAGMENTS" ]; then
    PARAM_FRAGMENTS="$PARAM_FRAGMENTS,"
  fi
  PARAM_FRAGMENTS="$PARAM_FRAGMENTS{\"parameter\":{\"name\":\"$_name\",\"value\":\"$_escaped_value\",\"sensitive\":$_sensitive}}"
}

add_param_fragment "$PB_NAME" "$PB_BUCKET" "$PB_SENSITIVE_FALSE"
add_param_fragment "$PB_HTTP_NAME" "$PB_HTTP_VALUE" "$PB_SENSITIVE_FALSE"
add_param_fragment "$PB_OZONE_NAME" "$PB_OZONE_ENDPOINT" "$PB_SENSITIVE_FALSE"
add_param_fragment "core_pg_host" "$CORE_PG_HOST" false
add_param_fragment "core_pg_port" "$CORE_PG_PORT" false
add_param_fragment "core_pg_user" "$CORE_PG_USER" false
add_param_fragment "core_pg_password" "$CORE_PG_PASSWORD" true
add_param_fragment "cms_pg_host" "$CMS_PG_HOST" false
add_param_fragment "cms_pg_port" "$CMS_PG_PORT" false
add_param_fragment "cms_pg_user" "$CMS_PG_USER" false
add_param_fragment "cms_pg_password" "$CMS_PG_PASSWORD" true
add_param_fragment "ozone_access_key" "$S3A_ACCESS_KEY" true
add_param_fragment "ozone_secret_key" "$S3A_SECRET_KEY" true

echo "Creating Parameter Context..."

# Bodies that carry secrets are sent on stdin so the values never appear in
# the process list.
CREATE_RESPONSE=$(printf '{"revision":{"version":0},"component":{"name":"%s","parameters":[%s]}}' \
    "$(json_escape "$PB_CONTEXT_NAME")" "$PARAM_FRAGMENTS" \
  | nifi_curl -s -w "\n%{http_code}" -X POST "$BASE_URL/parameter-contexts" \
    -H "Content-Type: application/json" \
    --data-binary @-)

HTTP_CODE=$(echo "$CREATE_RESPONSE" | tail -n1)
BODY=$(echo "$CREATE_RESPONSE" | sed '$d')

if [ "$HTTP_CODE" != "201" ] && [ "$HTTP_CODE" != "409" ]; then
  echo "Failed to create Parameter Context"
  echo "$BODY"
  exit 1
fi

echo "Parameter Context created or already exists"

echo "Fetching Parameter Context ID..."

# Retry until context appears
for _ in $(seq 1 10); do
  # Match the context entity itself: its component starts with "name", unlike
  # the referencing components and bound process groups NiFi nests inside it
  # once a flow uses the context (their ids would otherwise be picked up).
  PARAM_CONTEXT_ID=$(nifi_curl -s "$BASE_URL/flow/parameter-contexts" \
    | tr -d '\n' \
    | sed -n "s/.*\"id\":\"\([^\"]*\)\",\"permissions\":{[^}]*},\"component\":{\"name\":\"$PB_CONTEXT_NAME\",.*/\1/p" \
    | head -n 1)

  if [ -n "$PARAM_CONTEXT_ID" ]; then
    break
  fi

  echo "Waiting for Parameter Context to appear..."
  sleep 3
done

if [ -z "$PARAM_CONTEXT_ID" ]; then
  echo "Parameter Context still not found after retries"
  exit 1
fi

echo "Parameter Context ID: $PARAM_CONTEXT_ID"

echo "Ensuring required parameters exist in context..."

CONTEXT_RESPONSE=$(nifi_curl -s -w "\n%{http_code}" "$BASE_URL/parameter-contexts/$PARAM_CONTEXT_ID")
CONTEXT_HTTP_CODE=$(echo "$CONTEXT_RESPONSE" | tail -n1)
CONTEXT_BODY=$(echo "$CONTEXT_RESPONSE" | sed '$d')

if [ "$CONTEXT_HTTP_CODE" != "200" ]; then
  echo "Failed to fetch Parameter Context details"
  echo "$CONTEXT_BODY"
  exit 1
fi

UPDATED_PARAMETERS_JSON=""
PARAMS_UPDATED=false

# Ensure every required parameter exists in the context. Sensitive values are
# returned masked by NiFi, so existence is checked by name only, and only the
# MISSING parameters are sent on update (omitted parameters keep their values).
ensure_param() {
  _name="$1"
  _value="$2"
  _sensitive="$3"
  if echo "$CONTEXT_BODY" | tr -d '\n' | grep -q "\"name\":\"$_name\""; then
    return 0
  fi
  _escaped_value=$(json_escape "$_value")
  if [ -n "$UPDATED_PARAMETERS_JSON" ]; then
    UPDATED_PARAMETERS_JSON="$UPDATED_PARAMETERS_JSON,"
  fi
  UPDATED_PARAMETERS_JSON="$UPDATED_PARAMETERS_JSON{\"parameter\":{\"name\":\"$_name\",\"value\":\"$_escaped_value\",\"sensitive\":$_sensitive}}"
  PARAMS_UPDATED=true
}

ensure_param "$PB_NAME" "$PB_BUCKET" "$PB_SENSITIVE_FALSE"
ensure_param "$PB_HTTP_NAME" "$PB_HTTP_VALUE" "$PB_SENSITIVE_FALSE"
ensure_param "$PB_OZONE_NAME" "$PB_OZONE_ENDPOINT" "$PB_SENSITIVE_FALSE"
ensure_param "core_pg_host" "$CORE_PG_HOST" false
ensure_param "core_pg_port" "$CORE_PG_PORT" false
ensure_param "core_pg_user" "$CORE_PG_USER" false
ensure_param "core_pg_password" "$CORE_PG_PASSWORD" true
ensure_param "cms_pg_host" "$CMS_PG_HOST" false
ensure_param "cms_pg_port" "$CMS_PG_PORT" false
ensure_param "cms_pg_user" "$CMS_PG_USER" false
ensure_param "cms_pg_password" "$CMS_PG_PASSWORD" true
ensure_param "ozone_access_key" "$S3A_ACCESS_KEY" true
ensure_param "ozone_secret_key" "$S3A_SECRET_KEY" true

if [ "$PARAMS_UPDATED" = "true" ]; then
  # The context's own revision is the first one in the body; later ones belong
  # to referencing components.
  CONTEXT_REVISION_VERSION=$(echo "$CONTEXT_BODY" \
    | tr -d '\n' \
    | sed -n 's/^[[:space:]]*{[[:space:]]*"revision":[[:space:]]*{[^}]*"version":[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
    | head -n 1)

  if [ -z "$CONTEXT_REVISION_VERSION" ]; then
    echo "Failed to determine context revision version"
    exit 1
  fi

  UPDATE_RESPONSE=$(printf '{"revision":{"version":%s},"id":"%s","component":{"id":"%s","parameters":[%s]}}' \
      "$CONTEXT_REVISION_VERSION" "$PARAM_CONTEXT_ID" "$PARAM_CONTEXT_ID" "$UPDATED_PARAMETERS_JSON" \
    | nifi_curl -s -w "\n%{http_code}" -X PUT "$BASE_URL/parameter-contexts/$PARAM_CONTEXT_ID" \
      -H "Content-Type: application/json" \
      --data-binary @-)

  UPDATE_HTTP_CODE=$(echo "$UPDATE_RESPONSE" | tail -n1)
  UPDATE_BODY=$(echo "$UPDATE_RESPONSE" | sed '$d')

  if [ "$UPDATE_HTTP_CODE" != "200" ]; then
    echo "Failed to update required parameters in context"
    echo "$UPDATE_BODY"
    exit 1
  fi

  echo "Required parameters were added to existing context"
else
  echo "Required parameters already exist"
fi

echo "Fetching Root Process Group ID..."

ROOT_PG_ID=$(nifi_curl -s "$BASE_URL/flow/process-groups/root" \
  | tr -d '\n' \
  | awk -F'"id":"' '{print $2}' \
  | cut -d'"' -f1 \
  | head -n 1)

if [ -z "$ROOT_PG_ID" ]; then
  echo "Failed to determine Root Process Group ID"
  exit 1
fi

echo "Root PG ID: $ROOT_PG_ID"

TEMPLATE_TARGET_PG_ID="${NIFI_TEMPLATE_TARGET_PG_ID:-$ROOT_PG_ID}"

# The Parameter Context is bound to the group the template is imported into.
# NiFi does not inherit a parent group's context, so binding only root would
# leave the parameter references unresolved in a child target group.
echo "Fetching revision of process group $TEMPLATE_TARGET_PG_ID..."

# The process group's own revision is the first one in the body.
REVISION_VERSION=$(nifi_curl -s "$BASE_URL/process-groups/$TEMPLATE_TARGET_PG_ID" \
  | tr -d '\n' \
  | sed -n 's/^[[:space:]]*{[[:space:]]*"revision":[[:space:]]*{[^}]*"version":[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
  | head -n 1)

if [ -z "$REVISION_VERSION" ]; then
  echo "Failed to determine revision for process group: $TEMPLATE_TARGET_PG_ID"
  exit 1
fi

echo "Revision: $REVISION_VERSION"

echo "Applying Parameter Context..."

APPLY_RESPONSE=$(nifi_curl -s -w "\n%{http_code}" -X PUT "$BASE_URL/process-groups/$TEMPLATE_TARGET_PG_ID" \
  -H "Content-Type: application/json" \
  -d "{
    \"revision\": { \"version\": $REVISION_VERSION },
    \"component\": {
      \"id\": \"$TEMPLATE_TARGET_PG_ID\",
      \"parameterContext\": {
        \"id\": \"$PARAM_CONTEXT_ID\"
      }
    }
  }") || true
APPLY_HTTP_CODE=$(echo "$APPLY_RESPONSE" | tail -n1)

if [ "$APPLY_HTTP_CODE" != "200" ]; then
  echo "ERROR: applying Parameter Context to process group $TEMPLATE_TARGET_PG_ID failed (HTTP $APPLY_HTTP_CODE)"
  echo "$APPLY_RESPONSE" | sed '$d'
  exit 1
fi

echo "Parameter Context applied successfully"

# Initialization progress is recorded in NiFi itself, as the Parameter Context
# description, so it is removed together with the flow:
#   in-progress - template import started; enable/start not yet confirmed
#   complete    - initialization finished
# A flow without a marker (imported by hand or by an older init.sh) is treated
# like a completed one and left untouched.
INIT_STATE_IN_PROGRESS="tazama-init:in-progress"
INIT_STATE_COMPLETE="tazama-init:complete"

# Sets INIT_STATE (complete | in-progress | none) and INIT_CONTEXT_BODY.
read_init_state() {
  _response=$(nifi_curl -s -w "\n%{http_code}" "$BASE_URL/parameter-contexts/$PARAM_CONTEXT_ID") || true
  _code=$(echo "$_response" | tail -n1)
  INIT_CONTEXT_BODY=$(echo "$_response" | sed '$d')
  if [ "$_code" != "200" ]; then
    echo "ERROR: could not read Parameter Context $PARAM_CONTEXT_ID (HTTP $_code)"
    echo "$INIT_CONTEXT_BODY"
    exit 1
  fi
  case "$INIT_CONTEXT_BODY" in
    *"\"description\":\"$INIT_STATE_COMPLETE\""*) INIT_STATE=complete ;;
    *"\"description\":\"$INIT_STATE_IN_PROGRESS\""*) INIT_STATE=in-progress ;;
    *) INIT_STATE=none ;;
  esac
}

# Records the given marker as the Parameter Context description.
set_init_state() {
  read_init_state
  _revision=$(echo "$INIT_CONTEXT_BODY" \
    | tr -d '\n' \
    | sed -n 's/^[[:space:]]*{[[:space:]]*"revision":[[:space:]]*{[^}]*"version":[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
    | head -n 1)
  if [ -z "$_revision" ]; then
    echo "ERROR: could not determine Parameter Context revision"
    exit 1
  fi
  _response=$(nifi_curl -s -w "\n%{http_code}" -X PUT "$BASE_URL/parameter-contexts/$PARAM_CONTEXT_ID" \
    -H "Content-Type: application/json" \
    -d "{\"revision\":{\"version\":$_revision},\"id\":\"$PARAM_CONTEXT_ID\",\"component\":{\"id\":\"$PARAM_CONTEXT_ID\",\"description\":\"$1\"}}") || true
  _code=$(echo "$_response" | tail -n1)
  if [ "$_code" != "200" ]; then
    echo "ERROR: could not record initialization state '$1' (HTTP $_code)"
    echo "$_response" | sed '$d'
    exit 1
  fi
}

IMPORT_NIFI_TEMPLATE="${IMPORT_NIFI_TEMPLATE:-true}"
TEMPLATE_ALREADY_PRESENT=false
RESUME_INIT=false

if [ "$IMPORT_NIFI_TEMPLATE" = "true" ]; then
  TEMPLATE_FILE="${NIFI_TEMPLATE_FILE:-/nifi/tazama.xml}"
  TEMPLATE_X="${NIFI_TEMPLATE_X:-0.0}"
  TEMPLATE_Y="${NIFI_TEMPLATE_Y:-0.0}"

  if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "Template import enabled but file not found: $TEMPLATE_FILE"
    exit 1
  fi

  # Idempotency guard: if the target process group already has controller
  # services, the flow was imported before. Skip upload + instantiation so a
  # re-run never duplicates the canvas. A failed read must not be mistaken for
  # an empty group, or the flow would be imported a second time. Services of
  # parent groups are excluded (NiFi includes them by default), so an empty
  # child target is not mistaken for an imported one.
  EXISTING_RESPONSE=$(nifi_curl -s -w "\n%{http_code}" "$BASE_URL/flow/process-groups/$TEMPLATE_TARGET_PG_ID/controller-services?includeAncestorGroups=false") || true
  EXISTING_HTTP_CODE=$(echo "$EXISTING_RESPONSE" | tail -n1)
  EXISTING_SERVICES=$(echo "$EXISTING_RESPONSE" | sed '$d')
  if [ "$EXISTING_HTTP_CODE" != "200" ] || ! echo "$EXISTING_SERVICES" | tr -d '\n' | grep -q '"controllerServices"'; then
    echo "ERROR: could not list controller services in process group $TEMPLATE_TARGET_PG_ID (HTTP $EXISTING_HTTP_CODE)"
    echo "$EXISTING_SERVICES"
    exit 1
  fi

  if echo "$EXISTING_SERVICES" | tr -d '\n' | grep -q '"controllerServices":\[{'; then
    TEMPLATE_ALREADY_PRESENT=true
    read_init_state
    if [ "$INIT_STATE" = "in-progress" ]; then
      echo "Flow present but a previous initialization did not complete - resuming"
      RESUME_INIT=true
    else
      echo "Flow already present in process group $TEMPLATE_TARGET_PG_ID - skipping template import"
    fi
  else
    set_init_state "$INIT_STATE_IN_PROGRESS"
  fi

  if [ "$TEMPLATE_ALREADY_PRESENT" = "false" ]; then
  TEMPLATE_NAME=$(tr -d '\n' < "$TEMPLATE_FILE" | sed -n 's:.*<name>[[:space:]]*\([^<]*\)[[:space:]]*</name>.*:\1:p' | head -n 1)
  TEMPLATE_ID=""

  echo "Uploading NiFi template from $TEMPLATE_FILE ..."

  UPLOAD_RESPONSE=$(nifi_curl -s -w "\n%{http_code}" -X POST \
    "$BASE_URL/process-groups/$TEMPLATE_TARGET_PG_ID/templates/upload" \
    -H "Content-Type: multipart/form-data" \
    -F "template=@$TEMPLATE_FILE")

  UPLOAD_HTTP_CODE=$(echo "$UPLOAD_RESPONSE" | tail -n1)
  UPLOAD_BODY=$(echo "$UPLOAD_RESPONSE" | sed '$d')

  if [ "$UPLOAD_HTTP_CODE" != "201" ] && [ "$UPLOAD_HTTP_CODE" != "200" ]; then
    if echo "$UPLOAD_BODY" | grep -qi "already exists"; then
      echo "Template already exists in NiFi, reusing existing template ID"

      EXISTING_TEMPLATE_NAME=$(echo "$UPLOAD_BODY" | sed -n "s/.*template named '\([^']*\)'.*/\1/p" | head -n 1)
      if [ -z "$EXISTING_TEMPLATE_NAME" ]; then
        EXISTING_TEMPLATE_NAME="$TEMPLATE_NAME"
      fi

      TEMPLATES_BODY=$(nifi_curl -s "$BASE_URL/flow/templates")
      TEMPLATE_ID=$(echo "$TEMPLATES_BODY" \
        | tr -d '\n' \
        | sed 's/},{/}\n{/g' \
        | grep "\"name\":\"$EXISTING_TEMPLATE_NAME\"" \
        | sed -n 's/.*"template":{[^}]*"id":"\([^"]*\)".*/\1/p' \
        | head -n 1)

      if [ -z "$TEMPLATE_ID" ]; then
        TEMPLATE_ID=$(echo "$TEMPLATES_BODY" \
          | tr -d '\n' \
          | sed 's/},{/}\n{/g' \
          | grep "\"name\":\"$EXISTING_TEMPLATE_NAME\"" \
        | sed -n 's/.*"id":"\([^"]*\)".*/\1/p' \
        | head -n 1)
      fi

      if [ -z "$TEMPLATE_ID" ]; then
        echo "Template exists but could not resolve template ID for name: $EXISTING_TEMPLATE_NAME"
        echo "$UPLOAD_BODY"
        exit 1
      fi
    else
      echo "Template upload failed"
      echo "$UPLOAD_BODY"
      exit 1
    fi
  fi

  if [ -z "$TEMPLATE_ID" ]; then
    TEMPLATE_ID=$(echo "$UPLOAD_BODY" \
      | tr -d '\n' \
      | sed -n 's/.*"template"[[:space:]]*:[[:space:]]*{[^}]*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
      | head -n 1)

    if [ -z "$TEMPLATE_ID" ]; then
      TEMPLATE_ID=$(echo "$UPLOAD_BODY" \
        | tr -d '\n' \
        | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
        | head -n 1)
    fi
  fi

  # Fallback: the upload endpoint may answer with XML instead of JSON.
  if [ -z "$TEMPLATE_ID" ]; then
    TEMPLATE_ID=$(echo "$UPLOAD_BODY" \
      | tr -d '\n' \
      | sed -n 's/.*<id>\([^<]*\)<\/id>.*/\1/p' \
      | head -n 1)
  fi

  if [ -z "$TEMPLATE_ID" ]; then
    echo "Unable to extract template ID from upload response"
    echo "$UPLOAD_BODY"
    exit 1
  fi

  echo "Instantiating template ID: $TEMPLATE_ID"

  INSTANTIATE_RESPONSE=$(nifi_curl -s -w "\n%{http_code}" -X POST \
    "$BASE_URL/process-groups/$TEMPLATE_TARGET_PG_ID/template-instance" \
    -H "Content-Type: application/json" \
    -d "{
      \"templateId\": \"$TEMPLATE_ID\",
      \"originX\": $TEMPLATE_X,
      \"originY\": $TEMPLATE_Y
    }")

  INSTANTIATE_HTTP_CODE=$(echo "$INSTANTIATE_RESPONSE" | tail -n1)
  INSTANTIATE_BODY=$(echo "$INSTANTIATE_RESPONSE" | sed '$d')

  if [ "$INSTANTIATE_HTTP_CODE" != "201" ] && [ "$INSTANTIATE_HTTP_CODE" != "200" ] && [ "$INSTANTIATE_HTTP_CODE" != "409" ]; then
    echo "Template instantiation failed"
    echo "$INSTANTIATE_BODY"
    exit 1
  fi

  echo "Template imported and instantiated successfully"
  fi
else
  echo "Template import skipped (IMPORT_NIFI_TEMPLATE=false)"
fi

# Enable services and start the flow for a newly instantiated template, or to
# resume an initialization that did not complete (for example when nifi-init
# is restarted by "restart: on-failure" after a failed start). Once
# initialization has completed, the operator's current state - including a
# deliberately stopped flow - is preserved, so no state changes are issued.
#
# Every curl below ends in "|| true": with "set -e" a transport failure
# (connection refused/reset) would otherwise end the script instead of going
# through the retry handling. curl then reports HTTP code 000.
if [ "$IMPORT_NIFI_TEMPLATE" = "true" ] && { [ "$TEMPLATE_ALREADY_PRESENT" != "true" ] || [ "$RESUME_INIT" = "true" ]; }; then
  if [ "${NIFI_AUTO_START:-true}" = "true" ]; then
    echo "Enabling controller services..."

    ENABLE_RETRIES="${NIFI_ENABLE_RETRIES:-30}"
    ENABLE_DELAY_SECONDS="${NIFI_ENABLE_DELAY_SECONDS:-5}"
    ENABLE_ATTEMPT=1
    SERVICES_ENABLED=false

    while [ "$ENABLE_ATTEMPT" -le "$ENABLE_RETRIES" ]; do
      ENABLE_RESPONSE=$(nifi_curl -s -w "\n%{http_code}" -X PUT "$BASE_URL/flow/process-groups/$TEMPLATE_TARGET_PG_ID/controller-services" \
        -H "Content-Type: application/json" \
        -d "{\"id\": \"$TEMPLATE_TARGET_PG_ID\", \"state\": \"ENABLED\"}") || true
      ENABLE_HTTP_CODE=$(echo "$ENABLE_RESPONSE" | tail -n1)
      ENABLE_BODY=$(echo "$ENABLE_RESPONSE" | sed '$d')

      if [ "$ENABLE_HTTP_CODE" != "200" ]; then
        echo "WARNING: enable request returned HTTP $ENABLE_HTTP_CODE (attempt $ENABLE_ATTEMPT/$ENABLE_RETRIES)"
        echo "$ENABLE_BODY"
      fi

      SERVICES_RESPONSE=$(nifi_curl -s -w "\n%{http_code}" "$BASE_URL/flow/process-groups/$TEMPLATE_TARGET_PG_ID/controller-services?includeAncestorGroups=false") || true
      SERVICES_HTTP_CODE=$(echo "$SERVICES_RESPONSE" | tail -n1)
      SERVICES_BODY=$(echo "$SERVICES_RESPONSE" | sed '$d')

      # Only trust a successful, well-formed response. A failed or empty
      # read must not be mistaken for "all services enabled".
      if [ "$SERVICES_HTTP_CODE" != "200" ] || ! echo "$SERVICES_BODY" | tr -d '\n' | grep -q '"controllerServices"'; then
        echo "WARNING: controller-services query returned HTTP $SERVICES_HTTP_CODE (attempt $ENABLE_ATTEMPT/$ENABLE_RETRIES), retrying"
        ENABLE_ATTEMPT=$((ENABLE_ATTEMPT + 1))
        sleep "$ENABLE_DELAY_SECONDS"
        continue
      fi

      # Count each service's own run status. The component "state" fields
      # also appear for the processors that reference a service, so they
      # cannot be used to tell whether the services themselves are enabled.
      SERVICES_TOTAL=$(echo "$SERVICES_BODY" | tr -d '\n' | grep -o '"runStatus":"[A-Z]*"' | wc -l | tr -d ' ')
      SERVICES_ENABLED_COUNT=$(echo "$SERVICES_BODY" | tr -d '\n' | grep -o '"runStatus":"ENABLED"' | wc -l | tr -d ' ')
      NOT_ENABLED_COUNT=$((SERVICES_TOTAL - SERVICES_ENABLED_COUNT))

      if [ "$SERVICES_TOTAL" -gt 0 ] && [ "$NOT_ENABLED_COUNT" -eq 0 ]; then
        echo "All $SERVICES_TOTAL controller services are ENABLED"
        SERVICES_ENABLED=true
        break
      fi

      echo "Waiting for controller services to enable (attempt $ENABLE_ATTEMPT/$ENABLE_RETRIES, $NOT_ENABLED_COUNT of $SERVICES_TOTAL pending)..."
      ENABLE_ATTEMPT=$((ENABLE_ATTEMPT + 1))
      sleep "$ENABLE_DELAY_SECONDS"
    done

    if [ "$SERVICES_ENABLED" != "true" ]; then
      echo "ERROR: controller services did not reach ENABLED after $ENABLE_RETRIES attempts - not starting the flow"
      exit 1
    fi

    echo "Starting flow..."

    START_RETRIES="${NIFI_START_RETRIES:-10}"
    START_ATTEMPT=1
    FLOW_STARTED=false

    while [ "$START_ATTEMPT" -le "$START_RETRIES" ]; do
      START_RESPONSE=$(nifi_curl -s -w "\n%{http_code}" -X PUT "$BASE_URL/flow/process-groups/$TEMPLATE_TARGET_PG_ID" \
        -H "Content-Type: application/json" \
        -d "{\"id\": \"$TEMPLATE_TARGET_PG_ID\", \"state\": \"RUNNING\"}") || true
      START_HTTP_CODE=$(echo "$START_RESPONSE" | tail -n1)
      START_BODY=$(echo "$START_RESPONSE" | sed '$d')

      if [ "$START_HTTP_CODE" != "200" ]; then
        echo "WARNING: flow start returned HTTP $START_HTTP_CODE (attempt $START_ATTEMPT/$START_RETRIES)"
        echo "$START_BODY"
      else
        # HTTP 200 only means the request was accepted: NiFi skips processors
        # that are still validating. Confirm nothing in the group is left
        # stopped or invalid before reporting success; otherwise start again.
        GROUP_RESPONSE=$(nifi_curl -s -w "\n%{http_code}" "$BASE_URL/process-groups/$TEMPLATE_TARGET_PG_ID") || true
        GROUP_HTTP_CODE=$(echo "$GROUP_RESPONSE" | tail -n1)
        GROUP_BODY=$(echo "$GROUP_RESPONSE" | sed '$d')
        RUNNING_COUNT=$(echo "$GROUP_BODY" | grep -o '"runningCount":[0-9]*' | head -n 1 | cut -d: -f2)
        STOPPED_COUNT=$(echo "$GROUP_BODY" | grep -o '"stoppedCount":[0-9]*' | head -n 1 | cut -d: -f2)
        INVALID_COUNT=$(echo "$GROUP_BODY" | grep -o '"invalidCount":[0-9]*' | head -n 1 | cut -d: -f2)

        if [ "$GROUP_HTTP_CODE" = "200" ] && [ "$STOPPED_COUNT" = "0" ] && [ "$INVALID_COUNT" = "0" ] \
          && [ -n "$RUNNING_COUNT" ] && [ "$RUNNING_COUNT" -gt 0 ]; then
          echo "Flow started successfully ($RUNNING_COUNT components running)"
          FLOW_STARTED=true
          break
        fi

        echo "Waiting for all components to start (attempt $START_ATTEMPT/$START_RETRIES, HTTP $GROUP_HTTP_CODE: running=${RUNNING_COUNT:-?} stopped=${STOPPED_COUNT:-?} invalid=${INVALID_COUNT:-?})..."
      fi

      START_ATTEMPT=$((START_ATTEMPT + 1))
      sleep "$ENABLE_DELAY_SECONDS"
    done

    if [ "$FLOW_STARTED" != "true" ]; then
      echo "ERROR: flow did not fully start after $START_RETRIES attempts"
      exit 1
    fi
  else
    echo "Auto enable/start skipped (NIFI_AUTO_START=false)"
  fi

  set_init_state "$INIT_STATE_COMPLETE"
  echo "Initialization recorded as complete"
elif [ "$TEMPLATE_ALREADY_PRESENT" = "true" ]; then
  echo "Flow already present - leaving services and flow state untouched (use the NiFi UI to manage an existing flow)"
fi

echo "Init script finished"
