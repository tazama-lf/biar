# NiFi image

`FROM apache/nifi:1.24.0` with the PostgreSQL JDBC driver and a start-up wrapper (`entrypoint.sh`) that selects how NiFi authenticates users. The wrapper configures NiFi and then hands over to the stock `/opt/nifi/scripts/start.sh`, so every stock `apache/nifi` environment variable still applies.

## Authentication modes

| `NIFI_AUTH`      | Protocol         | Authentication                                    | Use for                                   |
| ---------------- | ---------------- | ------------------------------------------------- | ----------------------------------------- |
| `none` (default) | HTTP, port `NIFI_WEB_HTTP_PORT` (8088 in this repository's compose and env files) | None. Every client has full control of NiFi. | Local development on your own machine |
| `keycloak`       | HTTPS, port 8443 | Keycloak login (OIDC); access by Keycloak group   | Any deployment other people can reach     |

In `none` mode, if `NIFI_WEB_HTTP_PORT` is not set, the stock image behaviour applies: HTTPS on 8443 with the stock single-user login.

> [!WARNING]
> In `none` mode NiFi performs no authentication or authorization. Anyone who can reach the port can read the data flowing through NiFi (all tenants), change the flow, and run commands inside the container. Publish the port to `127.0.0.1` only. The container prints a warning banner at start-up in this mode.

The `ListenHTTP` content listener on port 8081 is part of the flow, not of the NiFi UI/API, and is the same in both modes.

## `none` mode

Unchanged stock behaviour: set `NIFI_WEB_HTTP_PORT` (and usually `NIFI_WEB_HTTP_HOST=0.0.0.0`). If the `conf` volume was previously used in `keycloak` mode, the wrapper clears the OIDC settings so NiFi starts on plain HTTP again.

## `keycloak` mode

### Required variables

The container exits at start-up with a list of every missing variable if any of these is empty or a placeholder (`changeme`, `<...>`, `your-...`, etc.):

| Variable                   | Purpose                                                                                                                              |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `KEYCLOAK_ISSUER_URL`      | Realm issuer, e.g. `https://keycloak.example.org/realms/tazama`. NiFi must be able to reach it; browsers are redirected to it.       |
| `KEYCLOAK_CLIENT_SECRET`   | Secret of the confidential Keycloak client.                                                                                          |
| `NIFI_WEB_PROXY_HOST`      | Comma-separated `host[:port]` list that browsers use to reach NiFi, e.g. `nifi.example.org,nifi.example.org:443`. NiFi rejects other `Host` headers. |
| `NIFI_SENSITIVE_PROPS_KEY` | Key that encrypts sensitive flow properties (passwords, keys). At least 12 characters. Keep it stable: see [Sensitive properties key](#sensitive-properties-key). |
| `KEYSTORE_PASSWORD`        | Password of the server keystore (generated or supplied).                                                                             |
| `TRUSTSTORE_PASSWORD`      | Password of the truststore (generated or supplied).                                                                                  |

Secret values must not contain `|`, `&` or `\` (the stock scripts write them with `sed`).

### Optional variables

| Variable                                                  | Default                       | Purpose                                                                                  |
| --------------------------------------------------------- | ----------------------------- | ---------------------------------------------------------------------------------------- |
| `KEYCLOAK_CLIENT_ID`                                      | `nifi`                        | Keycloak client ID                                                                       |
| `NIFI_ADMIN_GROUP`                                        | `NIFI_ADMIN`                  | Group name (in the groups claim) that gets full NiFi admin rights. Empty disables this.  |
| `NIFI_SECURITY_USER_OIDC_CLAIM_GROUPS`                    | `roles`                       | Token claim that lists the user's groups                                                 |
| `NIFI_SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER`          | `preferred_username`          | Claim used as the NiFi user name                                                         |
| `NIFI_SECURITY_USER_OIDC_FALLBACK_CLAIMS_IDENTIFYING_USER` | `email`                       | Fallback when the identifying claim is missing                                           |
| `NIFI_SECURITY_USER_OIDC_ADDITIONAL_SCOPES`               | `offline_access,profile`      | Extra scopes requested at login                                                          |
| `NIFI_WEB_HTTPS_PORT` / `NIFI_WEB_HTTPS_HOST`             | `8443` / `0.0.0.0`            | HTTPS listener                                                                           |
| `NIFI_TLS_SAN`                                            | (none)                        | Extra comma-separated host names/IPs for the generated server certificate                |
| `NIFI_TLS_DIR`                                            | `$NIFI_HOME/conf/tls`         | Where generated certificates are kept                                                    |
| `NIFI_TLS_CLIENT_DIR`                                     | `/opt/nifi/machine-identity`  | Where the machine identity certificate is written. Keep the default with this repository's `docker-compose.yaml`: the shared volume is mounted there and `init.sh` reads that path. |
| `KEYSTORE_PATH` / `KEYSTORE_TYPE`                         | generated / `PKCS12`          | Supply your own server keystore                                                          |
| `TRUSTSTORE_PATH` / `TRUSTSTORE_TYPE`                     | generated / `PKCS12`          | Supply your own truststore                                                               |
| `NIFI_ADMIN_GROUP_TIMEOUT`                                | `900`                         | Seconds the admin group set-up waits for NiFi to start                                   |

`NIFI_WEB_HTTP_PORT` and `NIFI_WEB_HTTP_HOST` are ignored in this mode, so an existing env file can be switched by changing `NIFI_AUTH` and adding the required variables. `INITIAL_ADMIN_IDENTITY` is set by the image; setting it to anything else is an error.

### Keycloak set-up

In the realm (for example `tazama`):

1. **Client:** confidential OpenID Connect client (`nifi`), standard flow only.
   Valid redirect URI: `https://<nifi-host>/nifi-api/access/oidc/callback`.
2. **Groups claim:** add a *User Realm Role* protocol mapper to the client with token claim name `roles`, multivalued, added to the ID token and userinfo. Keycloak does not emit realm roles as a flat claim by default.
3. **Group tree**, following the Tazama service-group convention: `tazama-nifi/NIFI_ADMIN/<ORG>` (for example `tazama-nifi/NIFI_ADMIN/TAZAMA.ORG`, with a `TENANT_ID` attribute on the organisation group). Create a realm role `NIFI_ADMIN` and map it onto the `NIFI_ADMIN` group; organisation subgroups inherit it.
4. Add operators to their organisation subgroup.

Users who can sign in to Keycloak but are not in `NIFI_ADMIN` get "Unable to view the user interface". To give other groups narrower access, create a NiFi user group with the same name as the Keycloak role (Settings > Users) and assign it policies in NiFi.

NiFi has no tenant isolation: there is one shared flow and its connection pools read every tenant's data. Only platform operators should be NiFi admins.

### What happens at start-up

1. **TLS.** Unless you supply them, the wrapper creates the following in `NIFI_TLS_DIR` (on the persisted `conf` volume):
   - An internal CA (10 years).
   - A server certificate (825 days) for `localhost`, `127.0.0.1`, the container host name, the `NIFI_WEB_PROXY_HOST` hosts and `NIFI_TLS_SAN`.
   - A PKCS12 keystore and truststore.

   The server certificate is reissued when the host list changes, the keystore password changes, or it is within 30 days of expiry. Browsers see a self-signed certificate unless a TLS-terminating proxy or load balancer sits in front.
2. **Machine identity.** A client certificate with subject `CN=tazama-nifi-machine` is written to `NIFI_TLS_CLIENT_DIR` as `machine.crt`, `machine.key` and `ca.crt`. This identity is NiFi's Initial Admin Identity. Automation that calls the NiFi API (such as the flow bootstrap `init.sh`) uses it:

   ```bash
   curl --cacert ca.crt --cert machine.crt --key machine.key https://nifi:8443/nifi-api/flow/current-user
   ```

   The files are owned by uid 1000 (`nifi`) and the key is mode `0600`. Treat `machine.key` as an admin credential.

   To give other containers the certificate, mount a named volume at `/opt/nifi/machine-identity` in NiFi and the same volume (read-only) in the consumer, which must run as uid 1000 or root. Docker gives a new named volume the directory's `nifi` ownership. Bind mounts and Kubernetes volumes are not initialised this way, so use a host directory owned by uid 1000, or `fsGroup: 1000`. Do not share the `conf` volume: it holds the internal CA key, the keystore and `nifi.properties` with the sensitive properties key and the OIDC client secret. Without a volume, the certificate is reissued (same identity, same CA) when the container is recreated.
3. **Sensitive properties key.** See [Sensitive properties key](#sensitive-properties-key).
4. **Admin group.** Once NiFi is up, a background job (`tazama-admin-group.sh`) creates the NiFi user group `NIFI_ADMIN_GROUP` and grants it every policy the machine identity holds. NiFi matches group names from the groups claim against its own groups, so Keycloak group members become admins without per-user NiFi changes. The job is idempotent and logs `admin group: done (N policies updated)`. It also grants the machine identity and the group read and write on the root process group and its data, which NiFi does not grant the Initial Admin on a fresh install. With an empty `NIFI_ADMIN_GROUP`, only the machine identity gets these.

### Supplying your own certificates

Mount the stores and set `KEYSTORE_PATH` and/or `TRUSTSTORE_PATH` (with the matching passwords and, if not PKCS12, the types). A supplied truststore must also trust `NIFI_TLS_DIR/ca.crt`, or the machine identity cannot authenticate. The internal CA and machine certificate are still generated.

### Sensitive properties key

NiFi encrypts sensitive flow properties with `nifi.sensitive.props.key`. In `none` mode NiFi generates a random key and stores it in `conf/nifi.properties`. When `keycloak` mode starts on a `conf` volume whose key differs from `NIFI_SENSITIVE_PROPS_KEY`, the wrapper re-encrypts the existing flow with the new key (`nifi.sh set-sensitive-properties-key`), so switching modes keeps controller-service passwords. After that, keep `NIFI_SENSITIVE_PROPS_KEY` unchanged. If you lose it, the sensitive values in the flow cannot be decrypted.

## Flow bootstrap (`init.sh`)

The `nifi-init` service (`curlimages/curl`, see `docker-compose.yaml`) runs `init.sh` once against the NiFi API. It:

1. Creates the `PB_CONTEXT_NAME` Parameter Context with the parameters below and binds it to the target process group `NIFI_TEMPLATE_TARGET_PG_ID` (default: root). Child groups do not inherit a parent's context. On an existing context it only adds missing parameters; it never overwrites existing values.
2. Imports `tazama.xml` into the target group, unless that group already has its own controller services.
3. With `NIFI_AUTO_START=true`, enables the controller services and starts the flow, and checks that no component is left stopped or invalid.

### Parameters

`tazama.xml` contains no environment-specific values. It references these parameters:

| Parameter                                                   | From                                                    | Sensitive |
| ----------------------------------------------------------- | ------------------------------------------------------- | --------- |
| `pbucket`, `phttp`, `pozone` (names set by `PB_*_NAME`)      | `PB_BUCKET`, `PB_HTTP_VALUE`, `PB_OZONE_ENDPOINT`        | no        |
| `core_pg_host`, `core_pg_port`, `core_pg_user`              | `CORE_PG_HOST`, `CORE_PG_PORT` (`15432`), `CORE_PG_USER` (`postgres`) | no |
| `core_pg_password`                                          | `CORE_PG_PASSWORD`                                      | yes       |
| `cms_pg_host`, `cms_pg_port`, `cms_pg_user`                 | `CMS_PG_HOST`, `CMS_PG_PORT` (`15433`), `CMS_PG_USER` (`postgres`) | no |
| `cms_pg_password`                                           | `CMS_PG_PASSWORD`                                       | yes       |
| `ozone_access_key`, `ozone_secret_key`                      | `S3A_ACCESS_KEY`, `S3A_SECRET_KEY`                      | yes       |

The core pools (`evaluation`, `enrichment`, `raw_history`, `event_history`, `configuration`) use the `core_pg_*` parameters; the `tazama_cms` pool uses `cms_pg_*`.

`init.sh` exits before it changes anything in NiFi if any value without a default is empty. NiFi would otherwise accept an empty host or password and the flow would only fail at runtime. To change a value after the first run, edit the parameter in the NiFi UI (*Parameter Contexts*); re-running `nifi-init` does not overwrite it.

### Other variables

| Variable                                           | Default                                   | Purpose                                                    |
| -------------------------------------------------- | ----------------------------------------- | ---------------------------------------------------------- |
| `IMPORT_NIFI_TEMPLATE`                             | `true`                                    | Import `NIFI_TEMPLATE_FILE` (`/nifi/tazama.xml`)            |
| `NIFI_AUTO_START`                                  | `true`                                    | Enable services and start the flow after a fresh import    |
| `NIFI_BASE_URL`                                    | `http://nifi:8088/nifi-api` (`none`), `https://nifi:8443/nifi-api` (`keycloak`) | NiFi API address. The ports follow `NIFI_WEB_HTTP_PORT` / `NIFI_WEB_HTTPS_PORT`. |
| `NIFI_API_WAIT_RETRIES` / `NIFI_API_WAIT_DELAY_SECONDS` | `60` / `5`                            | Wait for the API and for write access to the canvas        |
| `NIFI_ENABLE_RETRIES` / `NIFI_ENABLE_DELAY_SECONDS` | `30` / `5`                               | Wait for controller services to enable                     |
| `NIFI_START_RETRIES`                               | `10`                                      | Attempts to get every component running                    |

### Authentication

`init.sh` follows `NIFI_AUTH`. In `none` mode it calls the API over plain HTTP. In `keycloak` mode it calls `https://nifi:8443` with the [machine identity](#what-happens-at-start-up) certificate. For that, `docker-compose.yaml` mounts the `nifi_machine_identity` volume at `/opt/nifi/machine-identity` read-write in `nifi` and read-only in `nifi-init`, and runs `nifi-init` as uid 1000. `init.sh` always reads that path, so leave `NIFI_TLS_CLIENT_DIR` at its default. On a fresh install `init.sh` waits until the admin group job has granted the machine identity write access to the root process group.

### Re-runs

`init.sh` records its progress as the Parameter Context description (`tazama-init:in-progress`, then `tazama-init:complete`). If `nifi-init` is restarted after a failed enable or start, it resumes. After a completed run, and for flows imported by hand or by an older `init.sh` (no marker), it leaves services and flow state untouched, so a flow an operator stopped stays stopped.

### tazama-stack

The deployed bootstrap is a copy in `tazama-stack/biar/nifi/` (`init.sh` and `tazama.xml`), which needs a matching change when this template is synced. That copy creates only the three `PB_*` parameters and sets credentials after import as literal controller-service properties. With this template, all six DBCP pools would stay invalid on a fresh install there, because `#{core_pg_host}`, `#{core_pg_port}`, `#{core_pg_user}` and the `cms_pg_*` equivalents would be unresolved.

The matching change in `tazama-stack` needs to:

- create the parameters above, with this environment mapping:

  | tazama-stack                                    | biar `init.sh`                      |
  | ----------------------------------------------- | ----------------------------------- |
  | `TAZAMA_CORE_PG_PASSWORD`                       | `CORE_PG_PASSWORD`                  |
  | `TAZAMA_EXT_PG_PASSWORD`                        | `CMS_PG_PASSWORD`                   |
  | `TAZAMA_S3_ACCESS_KEY` / `TAZAMA_S3_SECRET_KEY` | `S3A_ACCESS_KEY` / `S3A_SECRET_KEY` |

  plus `CORE_PG_HOST`, `CORE_PG_PORT`, `CORE_PG_USER`, `CMS_PG_HOST`, `CMS_PG_PORT` and `CMS_PG_USER` for its own database hosts;
- drop the `set_service_properties` block, which would replace the parameter references with literals.

## Deployment

The `docker-compose.yaml` in this repository runs NiFi in `none` mode. For deployment wiring (localhost-only port binding in `none` mode, ports, secrets and the Keycloak client for `keycloak` mode) see `biar/README.md` in [tazama-stack](https://github.com/tazama-lf/tazama-stack).
