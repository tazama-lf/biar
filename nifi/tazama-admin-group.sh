#!/bin/bash
# Grants the Keycloak admin group (NIFI_ADMIN_GROUP, default NIFI_ADMIN) the
# same NiFi policies as the machine identity, NiFi's Initial Admin Identity in
# keycloak mode. NiFi matches the group names in the OIDC groups claim against
# its own groups by name, so members of the Keycloak group become NiFi admins
# without per-user changes in NiFi.
#
# Started in the background by entrypoint.sh; runs once per container start and
# is idempotent.
set -uo pipefail

group="${NIFI_ADMIN_GROUP-NIFI_ADMIN}"
machine='CN=tazama-nifi-machine'
api="https://localhost:${NIFI_WEB_HTTPS_PORT:-8443}/nifi-api"
client_dir="${NIFI_TLS_CLIENT_DIR}"
timeout="${NIFI_ADMIN_GROUP_TIMEOUT:-900}"

log() { echo "[tazama-nifi] admin group: $*"; }

if [ -z "${group}" ]; then
  log 'NIFI_ADMIN_GROUP is empty; skipping.'
  exit 0
fi

call() {
  local method="$1" path="$2" body="${3:-}"
  local -a args=(--silent --show-error --fail --max-time 30
    --cacert "${client_dir}/ca.crt" --cert "${client_dir}/machine.crt" --key "${client_dir}/machine.key"
    -X "${method}" "${api}${path}")
  if [ -n "${body}" ]; then
    args+=(-H 'Content-Type: application/json' --data "${body}")
  fi
  curl "${args[@]}"
}

# Wait for NiFi to accept the machine identity.
deadline=$(($(date +%s) + timeout))
until call GET /flow/current-user >/dev/null 2>&1; do
  if [ "$(date +%s)" -ge "${deadline}" ]; then
    log "NiFi API not reachable with the machine identity after ${timeout}s; giving up."
    exit 1
  fi
  sleep 10
done

groups_json="$(call GET /tenants/user-groups)" || {
  log 'cannot list user groups.'
  exit 1
}
group_id="$(jq -r --arg g "${group}" '.userGroups[] | select(.component.identity == $g) | .id' <<<"${groups_json}" | head -n1)"
if [ -z "${group_id}" ]; then
  body="$(jq -nc --arg g "${group}" '{revision: {version: 0}, component: {identity: $g}}')"
  group_id="$(call POST /tenants/user-groups "${body}" | jq -r '.id')" || {
    log "cannot create group ${group}."
    exit 1
  }
  log "created group ${group} (${group_id})."
fi

users_json="$(call GET /tenants/users)" || {
  log 'cannot list users.'
  exit 1
}
policy_ids="$(jq -r --arg m "${machine}" '.users[] | select(.component.identity == $m) | .component.accessPolicies[]?.id' <<<"${users_json}")"
if [ -z "${policy_ids}" ]; then
  log "machine identity ${machine} has no policies; nothing to copy."
  exit 1
fi

added=0
for id in ${policy_ids}; do
  policy="$(call GET "/policies/${id}")" || {
    log "cannot read policy ${id}."
    continue
  }
  if jq -e --arg gid "${group_id}" 'any(.component.userGroups[]?; .id == $gid)' <<<"${policy}" >/dev/null; then
    continue
  fi
  body="$(jq -c --arg gid "${group_id}" '{
      revision: .revision,
      component: {
        id: .component.id,
        resource: .component.resource,
        action: .component.action,
        users: [.component.users[]? | {id}],
        userGroups: ([.component.userGroups[]? | {id}] + [{id: $gid}])
      }
    }' <<<"${policy}")"
  if call PUT "/policies/${id}" "${body}" >/dev/null; then
    added=$((added + 1))
    log "granted $(jq -r '.component.action + " " + .component.resource' <<<"${policy}")"
  else
    log "cannot update policy ${id}."
  fi
done
log "done (${added} policies updated)."
