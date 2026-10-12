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

# NiFi seeds the root process group policies for the Initial Admin only if a
# flow already exists at its first secured start. On a fresh install neither the
# machine identity nor the admin group could see or change the canvas, so grant
# read and write on the root process group and its data explicitly.
ensure_root_policies() {
  local root_id users machine_id resource action policy body
  root_id="$(call GET /flow/process-groups/root | jq -r '.processGroupFlow.id')" || {
    log 'cannot read the root process group.'
    return 1
  }
  users="$(call GET /tenants/users)" || {
    log 'cannot list users.'
    return 1
  }
  machine_id="$(jq -r --arg m "${machine}" '.users[] | select(.component.identity == $m) | .id' <<<"${users}" | head -n1)"
  if [ -z "${machine_id}" ]; then
    log "machine identity ${machine} not found."
    return 1
  fi
  for resource in "/process-groups/${root_id}" "/data/process-groups/${root_id}"; do
    for action in read write; do
      # Returns the effective policy, which may be inherited from a parent
      # resource; only a policy on this exact resource counts.
      if policy="$(call GET "/policies/${action}${resource}" 2>/dev/null)" &&
        [ "$(jq -r '.component.resource' <<<"${policy}")" = "${resource}" ]; then
        if jq -e --arg m "${machine_id}" --arg g "${group_id}" \
          'any(.component.users[]?; .id == $m) and ($g == "" or any(.component.userGroups[]?; .id == $g))' \
          <<<"${policy}" >/dev/null; then
          continue
        fi
        body="$(jq -c --arg m "${machine_id}" --arg g "${group_id}" '{
            revision: .revision,
            component: {
              id: .component.id,
              resource: .component.resource,
              action: .component.action,
              users: ([.component.users[]? | {id}] + [{id: $m}] | unique_by(.id)),
              userGroups: ([.component.userGroups[]? | {id}] + (if $g == "" then [] else [{id: $g}] end) | unique_by(.id))
            }
          }' <<<"${policy}")"
        call PUT "/policies/$(jq -r '.component.id' <<<"${policy}")" "${body}" >/dev/null || {
          log "cannot update policy ${action} ${resource}."
          continue
        }
      else
        body="$(jq -nc --arg r "${resource}" --arg a "${action}" --arg m "${machine_id}" --arg g "${group_id}" '{
            revision: {version: 0},
            component: {
              resource: $r,
              action: $a,
              users: [{id: $m}],
              userGroups: (if $g == "" then [] else [{id: $g}] end)
            }
          }')"
        call POST /policies "${body}" >/dev/null || {
          log "cannot create policy ${action} ${resource}."
          continue
        }
      fi
      log "granted ${action} ${resource} (root process group)"
    done
  done
}

if [ -z "${group}" ]; then
  group_id=''
  ensure_root_policies
  log 'NIFI_ADMIN_GROUP is empty; no group set up.'
  exit 0
fi

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

ensure_root_policies

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
