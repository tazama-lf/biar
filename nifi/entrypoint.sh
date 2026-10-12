#!/bin/bash
# Tazama NiFi entrypoint.
#
# Selects the authentication mode (NIFI_AUTH=none|keycloak), then hands over to
# the stock Apache NiFi start script. See nifi/README.md for the variables.
set -euo pipefail

scripts_dir=/opt/nifi/scripts
# shellcheck source=/dev/null
. "${scripts_dir}/common.sh" # prop_replace

# Identity of the client certificate the image generates for automation.
# It becomes NiFi's Initial Admin Identity in keycloak mode.
readonly MACHINE_IDENTITY='CN=tazama-nifi-machine'

log() { echo "[tazama-nifi] $*"; }
die() {
  echo "[tazama-nifi] ERROR: $*" >&2
  exit 1
}

# prop_replace (stock common.sh) uses sed with '|' as the delimiter, so these
# characters would corrupt nifi.properties.
check_sed_safe() {
  local name="$1" value="$2"
  case "${value}" in
    *'|'* | *'&'* | *\\*) die "${name} must not contain '|', '&' or '\\'." ;;
  esac
}

is_placeholder() {
  local v
  v="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "${v}" in
    changeme | change_me | change-me | '<'*'>' | your-* | your_* | replace-me* | replace_me* | placeholder | todo | xxx*) return 0 ;;
  esac
  return 1
}

configure_none() {
  # A conf volume previously used in keycloak mode still has the OIDC settings;
  # clear them so NiFi does not try to enable OIDC on plain HTTP.
  prop_replace 'nifi.security.user.oidc.discovery.url' ''
  prop_replace 'nifi.security.user.oidc.client.id' ''
  prop_replace 'nifi.security.user.oidc.client.secret' ''

  if [ -n "${NIFI_WEB_HTTP_PORT:-}" ]; then
    cat >&2 <<EOF
[tazama-nifi] ##################################################################
[tazama-nifi] WARNING: NIFI_AUTH=none - NiFi is running over plain HTTP on port
[tazama-nifi] ${NIFI_WEB_HTTP_PORT} WITHOUT authentication or authorization. Anyone who can
[tazama-nifi] reach this port has full control of NiFi, including the data
[tazama-nifi] flowing through it and command execution inside this container.
[tazama-nifi] Use this mode for local development only, and never publish the
[tazama-nifi] port beyond the local machine. Set NIFI_AUTH=keycloak for any
[tazama-nifi] deployment that other people or networks can reach.
[tazama-nifi] ##################################################################
EOF
  fi
}

# Adds a subjectAltName entry for a host name or IP address.
san_entry() {
  local host="$1"
  host="${host#[}"
  host="${host%]}"
  if [[ "${host}" =~ ^[0-9.]+$ || "${host}" == *:* ]]; then
    printf 'IP:%s' "${host}"
  else
    printf 'DNS:%s' "${host}"
  fi
}

build_san_list() {
  local -a hosts=(localhost 127.0.0.1 "${HOSTNAME}")
  local entry host
  # Host names from the proxy host list (host[:port], comma separated) and
  # any additional names in NIFI_TLS_SAN.
  IFS=',' read -ra entries <<<"${NIFI_WEB_PROXY_HOST:-},${NIFI_TLS_SAN:-}"
  for entry in "${entries[@]}"; do
    entry="$(printf '%s' "${entry}" | tr -d '[:space:]')"
    [ -z "${entry}" ] && continue
    if [[ "${entry}" == \[* ]]; then
      host="${entry%%]*}]"
    else
      host="${entry%%:*}"
    fi
    hosts+=("${host}")
  done
  local -a sans=()
  for host in $(printf '%s\n' "${hosts[@]}" | awk 'NF && !seen[$0]++'); do
    sans+=("$(san_entry "${host}")")
  done
  local IFS=','
  printf '%s' "${sans[*]}"
}

# Signs a key pair with the internal CA.
issue_cert() {
  local key="$1" crt="$2" subject="$3" ext="$4"
  openssl req -new -newkey rsa:3072 -nodes -keyout "${key}" -subj "${subject}" -out "${key}.csr" 2>/dev/null
  printf '%s\n' "${ext}" >"${key}.ext"
  openssl x509 -req -in "${key}.csr" -CA "${tls_dir}/ca.crt" -CAkey "${tls_dir}/ca.key" \
    -CAcreateserial -days 825 -sha256 -extfile "${key}.ext" -out "${crt}" 2>/dev/null
  rm -f "${key}.csr" "${key}.ext"
  chmod 600 "${key}"
}

needs_renewal() {
  [ ! -f "$1" ] || ! openssl x509 -checkend 2592000 -noout -in "$1" >/dev/null 2>&1
}

ensure_tls() {
  tls_dir="${NIFI_TLS_DIR:-${NIFI_HOME}/conf/tls}"
  local client_dir="${NIFI_TLS_CLIENT_DIR:-${tls_dir}/client}"
  mkdir -p "${tls_dir}" "${client_dir}"
  chmod 700 "${tls_dir}"

  local ca_renewed=false
  if [ ! -f "${tls_dir}/ca.key" ] || needs_renewal "${tls_dir}/ca.crt"; then
    log "Generating internal certificate authority in ${tls_dir}"
    openssl req -x509 -newkey rsa:3072 -nodes -days 3650 -sha256 \
      -keyout "${tls_dir}/ca.key" -out "${tls_dir}/ca.crt" \
      -subj '/CN=Tazama NiFi internal CA' \
      -addext 'basicConstraints=critical,CA:TRUE' \
      -addext 'keyUsage=critical,keyCertSign,cRLSign' 2>/dev/null
    chmod 600 "${tls_dir}/ca.key"
    ca_renewed=true
  fi

  # Server keystore: generated unless KEYSTORE_PATH points at your own.
  if [ -n "${KEYSTORE_PATH:-}" ]; then
    [ -f "${KEYSTORE_PATH}" ] || die "KEYSTORE_PATH (${KEYSTORE_PATH}) does not exist."
    export KEYSTORE_TYPE="${KEYSTORE_TYPE:-PKCS12}"
    log "Using supplied keystore ${KEYSTORE_PATH}"
  else
    export KEYSTORE_PATH="${tls_dir}/keystore.p12" KEYSTORE_TYPE=PKCS12
    local san
    san="$(build_san_list)"
    if [ "${ca_renewed}" = true ] || needs_renewal "${tls_dir}/server.crt" ||
      [ "$(cat "${tls_dir}/server.san" 2>/dev/null)" != "${san}" ] ||
      ! keytool -list -keystore "${KEYSTORE_PATH}" -storetype PKCS12 -storepass:env KEYSTORE_PASSWORD >/dev/null 2>&1; then
      log "Generating self-signed server certificate (${san})"
      issue_cert "${tls_dir}/server.key" "${tls_dir}/server.crt" "/CN=${HOSTNAME}" \
        "subjectAltName=${san}
extendedKeyUsage=serverAuth,clientAuth
keyUsage=critical,digitalSignature,keyEncipherment"
      rm -f "${KEYSTORE_PATH}"
      openssl pkcs12 -export -name nifi-key -in "${tls_dir}/server.crt" -inkey "${tls_dir}/server.key" \
        -certfile "${tls_dir}/ca.crt" -out "${KEYSTORE_PATH}" -passout env:KEYSTORE_PASSWORD
      chmod 600 "${KEYSTORE_PATH}"
      printf '%s' "${san}" >"${tls_dir}/server.san"
    fi
  fi

  # Truststore: decides which client certificates NiFi accepts.
  if [ -n "${TRUSTSTORE_PATH:-}" ]; then
    [ -f "${TRUSTSTORE_PATH}" ] || die "TRUSTSTORE_PATH (${TRUSTSTORE_PATH}) does not exist."
    export TRUSTSTORE_TYPE="${TRUSTSTORE_TYPE:-PKCS12}"
    log "Using supplied truststore ${TRUSTSTORE_PATH}. It must trust ${tls_dir}/ca.crt, or the machine identity cannot authenticate."
  else
    export TRUSTSTORE_PATH="${tls_dir}/truststore.p12" TRUSTSTORE_TYPE=PKCS12
    if [ "${ca_renewed}" = true ] ||
      ! keytool -list -keystore "${TRUSTSTORE_PATH}" -storetype PKCS12 -storepass:env TRUSTSTORE_PASSWORD -alias tazama-nifi-ca >/dev/null 2>&1; then
      log 'Generating truststore'
      rm -f "${TRUSTSTORE_PATH}"
      keytool -importcert -noprompt -alias tazama-nifi-ca -file "${tls_dir}/ca.crt" \
        -keystore "${TRUSTSTORE_PATH}" -storetype PKCS12 -storepass:env TRUSTSTORE_PASSWORD >/dev/null
      chmod 600 "${TRUSTSTORE_PATH}"
    fi
  fi

  # Machine identity client certificate, for automation that calls the API.
  if [ "${ca_renewed}" = true ] || needs_renewal "${client_dir}/machine.crt" || [ ! -f "${client_dir}/machine.key" ]; then
    log "Generating machine identity certificate (${MACHINE_IDENTITY}) in ${client_dir}"
    issue_cert "${client_dir}/machine.key" "${client_dir}/machine.crt" "/${MACHINE_IDENTITY}" \
      "extendedKeyUsage=clientAuth
keyUsage=critical,digitalSignature,keyEncipherment"
  fi
  cp "${tls_dir}/ca.crt" "${client_dir}/ca.crt"
  chmod 644 "${client_dir}/ca.crt" "${client_dir}/machine.crt"

  export NIFI_TLS_DIR="${tls_dir}" NIFI_TLS_CLIENT_DIR="${client_dir}"
}

# NiFi encrypts sensitive flow properties with this key. If the persisted conf
# already holds a different key (e.g. one NiFi generated in none mode), migrate
# the flow to the new key instead of failing to decrypt it on start-up.
migrate_sensitive_props_key() {
  local current
  current="$(sed -n 's/^nifi\.sensitive\.props\.key=//p' "${NIFI_HOME}/conf/nifi.properties")"
  if [ -n "${current}" ] && [ "${current}" != "${NIFI_SENSITIVE_PROPS_KEY}" ]; then
    if [ -f "${NIFI_HOME}/conf/flow.json.gz" ] || [ -f "${NIFI_HOME}/conf/flow.xml.gz" ]; then
      log 'Re-encrypting the existing flow with NIFI_SENSITIVE_PROPS_KEY'
      "${NIFI_HOME}/bin/nifi.sh" set-sensitive-properties-key "${NIFI_SENSITIVE_PROPS_KEY}" >/dev/null
    fi
  fi
}

configure_keycloak() {
  local missing=() var
  for var in KEYCLOAK_ISSUER_URL KEYCLOAK_CLIENT_SECRET NIFI_WEB_PROXY_HOST \
    NIFI_SENSITIVE_PROPS_KEY KEYSTORE_PASSWORD TRUSTSTORE_PASSWORD; do
    if [ -z "${!var:-}" ]; then
      missing+=("${var}")
    elif is_placeholder "${!var}"; then
      missing+=("${var} (placeholder value)")
    fi
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    local list
    list="$(printf '%s, ' "${missing[@]}")"
    die "NIFI_AUTH=keycloak requires: ${list%, }"
  fi
  [[ "${KEYCLOAK_ISSUER_URL}" =~ ^https?:// ]] || die 'KEYCLOAK_ISSUER_URL must start with http:// or https://.'
  [ "${#NIFI_SENSITIVE_PROPS_KEY}" -ge 12 ] || die 'NIFI_SENSITIVE_PROPS_KEY must be at least 12 characters.'
  for var in KEYCLOAK_CLIENT_SECRET NIFI_SENSITIVE_PROPS_KEY KEYSTORE_PASSWORD TRUSTSTORE_PASSWORD; do
    check_sed_safe "${var}" "${!var}"
  done
  if [ -n "${INITIAL_ADMIN_IDENTITY:-}" ] && [ "${INITIAL_ADMIN_IDENTITY}" != "${MACHINE_IDENTITY}" ]; then
    die "INITIAL_ADMIN_IDENTITY is set by the image in keycloak mode (${MACHINE_IDENTITY}); remove it. Grant people access through the ${NIFI_ADMIN_GROUP:-NIFI_ADMIN} group instead."
  fi

  # HTTPS only: start.sh switches to HTTP whenever NIFI_WEB_HTTP_PORT is set.
  unset NIFI_WEB_HTTP_PORT NIFI_WEB_HTTP_HOST
  export NIFI_WEB_HTTPS_PORT="${NIFI_WEB_HTTPS_PORT:-8443}"
  export NIFI_WEB_HTTPS_HOST="${NIFI_WEB_HTTPS_HOST:-0.0.0.0}"
  export AUTH=oidc
  export INITIAL_ADMIN_IDENTITY="${MACHINE_IDENTITY}"

  export NIFI_SECURITY_USER_OIDC_DISCOVERY_URL="${KEYCLOAK_ISSUER_URL%/}/.well-known/openid-configuration"
  export NIFI_SECURITY_USER_OIDC_CLIENT_ID="${KEYCLOAK_CLIENT_ID:-nifi}"
  export NIFI_SECURITY_USER_OIDC_CLIENT_SECRET="${KEYCLOAK_CLIENT_SECRET}"
  export NIFI_SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER="${NIFI_SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER:-preferred_username}"
  export NIFI_SECURITY_USER_OIDC_FALLBACK_CLAIMS_IDENTIFYING_USER="${NIFI_SECURITY_USER_OIDC_FALLBACK_CLAIMS_IDENTIFYING_USER:-email}"
  export NIFI_SECURITY_USER_OIDC_ADDITIONAL_SCOPES="${NIFI_SECURITY_USER_OIDC_ADDITIONAL_SCOPES:-offline_access,profile}"
  # Not exposed by the stock scripts; start.sh leaves it untouched.
  prop_replace 'nifi.security.user.oidc.claim.groups' "${NIFI_SECURITY_USER_OIDC_CLAIM_GROUPS:-roles}"

  ensure_tls
  migrate_sensitive_props_key

  log "Keycloak mode: HTTPS on ${NIFI_WEB_HTTPS_PORT}, issuer ${KEYCLOAK_ISSUER_URL}, client ${NIFI_SECURITY_USER_OIDC_CLIENT_ID}"
  "${scripts_dir}/tazama-admin-group.sh" &
}

case "$(printf '%s' "${NIFI_AUTH:-none}" | tr '[:upper:]' '[:lower:]')" in
  none) configure_none ;;
  keycloak) configure_keycloak ;;
  *) die "NIFI_AUTH must be 'none' or 'keycloak' (got '${NIFI_AUTH}')." ;;
esac

exec "${scripts_dir}/start.sh"
