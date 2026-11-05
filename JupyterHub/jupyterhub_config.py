import os

c = get_config()  # noqa: F821

# --- Spawner: local processes, all on the same server ---
c.JupyterHub.spawner_class = "simple"
# Each user gets an isolated workspace under /srv/notebooks/{username}.
# Shared dashboards are symlinked read-only from /srv/shared_notebooks.
c.Spawner.notebook_dir = "/srv/notebooks/{username}"
c.Spawner.args = ["--allow-root"]
c.Spawner.default_url = "/lab"

# Spark/Java initialization can take >30s; give the notebook server more time.
c.Spawner.http_timeout = 120
c.Spawner.start_timeout = 120

# Pass environment variables from JupyterHub to each user's notebook server
c.Spawner.environment = {
    "SPARK_HOME": os.environ.get("SPARK_HOME", "/opt/spark"),
    "JAVA_HOME": os.environ.get("JAVA_HOME", "/opt/java"),
    "SPARK_JARS": os.environ.get(
        "SPARK_JARS", "/opt/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar"
    ),
    "S3A_ENDPOINT": os.environ.get("S3A_ENDPOINT", ""),
    "S3A_ACCESS_KEY": os.environ.get("S3A_ACCESS_KEY", ""),
    "S3A_SECRET_KEY": os.environ.get("S3A_SECRET_KEY", ""),
    "WAREHOUSE_ROOT": os.environ.get("WAREHOUSE_ROOT", "/opt/Tazama_Warehouse"),
    "SPARK_DRIVER_MEMORY": os.environ.get("SPARK_DRIVER_MEMORY", "4g"),
    "PYSPARK_PYTHON": "python3",
    "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
}

# --- Authentication ---
# Default: Keycloak OIDC via oauthenticator.GenericOAuthenticator, so JupyterHub
# users authenticate against the same Keycloak realm as the rest of the platform
# (see tazama-lf/biar#184). Set JUPYTERHUB_AUTH=native to fall back to the
# previous NativeAuthenticator (local signup/approval) behaviour.
if os.environ.get("JUPYTERHUB_AUTH", "keycloak").lower() == "native":
    c.JupyterHub.authenticator_class = "nativeauthenticator.NativeAuthenticator"

    # First user to sign up must be the admin (set via JUPYTERHUB_ADMIN env var)
    admin = os.environ.get("JUPYTERHUB_ADMIN", "admin")
    c.Authenticator.admin_users = {admin}

    # New signups require admin approval before they can log in.
    # NativeAuthenticator's own is_authorized flag (set to 0 on signup) is the
    # security gate - allow_all=True just prevents JupyterHub from adding a second,
    # conflicting block on top of NativeAuthenticator's own authorization check.
    c.NativeAuthenticator.open_signup = False
    c.Authenticator.allow_all = True
else:
    from oauthenticator.generic import GenericOAuthenticator

    # e.g. https://keycloak.example.org/realms/tazama
    KC_BASE = os.environ.get("KEYCLOAK_ISSUER_URL", "").rstrip("/")
    # Externally visible JupyterHub URL, used for the OAuth callback,
    # e.g. https://jupyter.example.org
    PUBLIC_URL = os.environ.get("JUPYTERHUB_PUBLIC_URL", "").rstrip("/")

    # Fail fast on missing configuration: with empty values the hub would
    # start fine but every login would fail with opaque redirect/token errors.
    _required = [
        "KEYCLOAK_ISSUER_URL",
        "JUPYTERHUB_PUBLIC_URL",
        "KEYCLOAK_CLIENT_SECRET",
        "JUPYTERHUB_CRYPT_KEY",
    ]
    _missing = [n for n in _required if not os.environ.get(n)]
    if _missing:
        raise RuntimeError(
            "Keycloak auth (JUPYTERHUB_AUTH=keycloak) requires environment "
            "variables: " + ", ".join(_missing)
        )

    import contextvars
    import json
    from urllib.parse import urlencode

    from oauthenticator.oauth2 import OAuthLogoutHandler
    from tornado.httpclient import HTTPClientError

    # Set by get_token_info only when Keycloak confirms the refresh token is
    # dead (OAuth error "invalid_grant"). Every other failure (Keycloak down,
    # 5xx, invalid_client after a secret rotation, a userinfo error after a
    # successful refresh) leaves it unset, so running servers are kept.
    # Each hub request runs in its own asyncio task, so this is per-refresh.
    _refresh_token_rejected = contextvars.ContextVar(
        "refresh_token_rejected", default=False
    )

    def _oauth_error(e):
        """Return the OAuth `error` field of a failed token response, or ""."""
        if e.response is None or not e.response.body:
            return ""
        try:
            return json.loads(e.response.body).get("error", "")
        except (ValueError, AttributeError):
            return ""

    class KeycloakLogoutHandler(OAuthLogoutHandler):
        """Logout handler that also ends the user's Keycloak SSO session.

        The stock handler only clears the hub's own cookies. The browser keeps
        its Keycloak session cookie, so the next "Sign in with Keycloak" is
        answered by Keycloak without a credential prompt and the previous user
        is silently signed back in (tazama-lf/biar#215). Here the hub ends the
        Keycloak session server-to-server with the user's refresh token, so no
        Keycloak confirmation page is shown.

        Ending the session also invalidates the stored refresh token: if the
        user's server is left running, its next auth refresh gets invalid_grant
        and KeycloakOAuthenticator.refresh_user stops it.
        """

        async def handle_logout(self):
            # JupyterHub has already cleared the hub login cookie here, but
            # current_user is still set until the logout page is rendered.
            user = self.current_user
            if user is not None:
                await self._end_keycloak_session(user)
            # Clears the OAuth state cookie.
            await super().handle_logout()

        async def _end_keycloak_session(self, user):
            auth_state = await user.get_auth_state() or {}
            refresh_token = auth_state.get("refresh_token")
            if not refresh_token:
                self.log.info(
                    f"No refresh_token in auth_state for {user.name}; "
                    "clearing local cookies only"
                )
                return
            authenticator = self.authenticator
            try:
                await authenticator.httpfetch(
                    f"{KC_BASE}/protocol/openid-connect/logout",
                    label="ending Keycloak session",
                    parse_json=False,
                    method="POST",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    body=urlencode(
                        {
                            "client_id": authenticator.client_id,
                            "client_secret": authenticator.client_secret,
                            "refresh_token": refresh_token,
                        }
                    ),
                    request_timeout=10,
                )
            except (HTTPClientError, OSError) as e:
                # Never block logout on Keycloak: the hub cookies are already
                # cleared, the user just keeps their Keycloak session.
                code = getattr(e, "code", type(e).__name__)
                self.log.warning(
                    f"Keycloak end-session failed for {user.name} ({code}); "
                    "local cookies cleared anyway"
                )
                return
            self.log.info(f"Ended Keycloak session for {user.name}")

    class KeycloakOAuthenticator(GenericOAuthenticator):
        """GenericOAuthenticator that ends the session when Keycloak revokes it.

        When Keycloak rejects the refresh token (session timeout, admin logout,
        password change), the stock refresh_user only returns False. The user's
        single-user server keeps running and keeps calling the hub, and every
        call retries the dead refresh token, forever (tazama-lf/biar#209).
        Here we stop the user's servers and clear the hub cookie instead, so
        the next visit goes through a fresh Keycloak login.

        Logging out of JupyterHub also ends the Keycloak session, see
        KeycloakLogoutHandler (tazama-lf/biar#215).
        """

        logout_handler = KeycloakLogoutHandler

        async def get_token_info(self, handler, params):
            try:
                return await super().get_token_info(handler, params)
            except HTTPClientError as e:
                # Only invalid_grant means the token itself is dead. Other 4xx
                # errors (invalid_client, invalid_request, ...) are config
                # problems and must not stop every notebook in the fleet.
                # Also called on login, where the flag is never read.
                if 400 <= e.code < 500 and _oauth_error(e) == "invalid_grant":
                    _refresh_token_rejected.set(True)
                raise

        async def refresh_user(self, user, handler=None, **kwargs):
            _refresh_token_rejected.set(False)
            result = await super().refresh_user(user, handler=handler, **kwargs)
            if result is not False:
                return result
            if not _refresh_token_rejected.get():
                # Not a confirmed revocation (Keycloak outage, userinfo error,
                # client misconfiguration): don't kill running notebooks. The
                # request is still denied and the next refresh retries.
                self.log.warning(
                    f"Refreshing auth for {user.name} failed without a "
                    "confirmed token revocation; leaving servers running"
                )
                return result

            active = [
                name
                for name, spawner in user.spawners.items()
                if spawner.active and not spawner.pending
            ]
            self.log.warning(
                f"Keycloak session for {user.name} is no longer valid; "
                f"stopping servers {active} and requiring a fresh login"
            )
            for server_name in active:
                try:
                    if handler is not None:
                        # Also removes the proxy route, unlike user.stop()
                        await handler.stop_single_user(user, server_name)
                    else:
                        await user.stop(server_name)
                except Exception:
                    self.log.exception(
                        f"Failed to stop server {server_name!r} for {user.name}"
                    )
            if handler is not None:
                handler.clear_login_cookie()
            return result

    c.JupyterHub.authenticator_class = KeycloakOAuthenticator
    c.GenericOAuthenticator.login_service = "Keycloak"
    c.GenericOAuthenticator.client_id = os.environ.get(
        "KEYCLOAK_CLIENT_ID", "jupyterhub"
    )
    c.GenericOAuthenticator.client_secret = os.environ.get("KEYCLOAK_CLIENT_SECRET", "")
    c.GenericOAuthenticator.oauth_callback_url = f"{PUBLIC_URL}/hub/oauth_callback"
    c.GenericOAuthenticator.authorize_url = f"{KC_BASE}/protocol/openid-connect/auth"
    c.GenericOAuthenticator.token_url = f"{KC_BASE}/protocol/openid-connect/token"
    c.GenericOAuthenticator.userdata_url = f"{KC_BASE}/protocol/openid-connect/userinfo"
    c.GenericOAuthenticator.username_claim = "preferred_username"
    c.GenericOAuthenticator.scope = ["openid", "profile", "email"]

    # Authorization comes from Keycloak realm roles, surfaced as a flat `roles`
    # claim in the userinfo response by the realm's `realm-roles-userinfo`
    # protocol mapper (Keycloak does NOT include realm roles in userinfo by
    # default). Users receive the roles through membership of the
    # tazama-jupyter/JUPYTER_ADMIN|JUPYTER_USER/<org> group tree.
    # Note: auth_state_groups_key requires manage_groups=True (oauthenticator
    # 17.x raises at startup otherwise), and enable_auth_state requires
    # JUPYTERHUB_CRYPT_KEY to be set (generate with: openssl rand -hex 32).
    c.GenericOAuthenticator.enable_auth_state = True
    c.GenericOAuthenticator.manage_groups = True
    c.GenericOAuthenticator.auth_state_groups_key = "oauth_user.roles"
    c.GenericOAuthenticator.allowed_groups = {"JUPYTER_USER", "JUPYTER_ADMIN"}
    c.GenericOAuthenticator.admin_groups = {"JUPYTER_ADMIN"}

# --- Networking ---
c.JupyterHub.ip = "0.0.0.0"
c.JupyterHub.port = 8000

# --- Persistence ---
c.JupyterHub.cookie_secret_file = "/data/jupyterhub_cookie_secret"
c.JupyterHub.db_url = "sqlite:////data/jupyterhub.sqlite"


# Create per-user workspace and symlink shared notebooks into it.
# SimpleLocalProcessSpawner runs as root - no system user creation needed.
# Email-style usernames (e.g. user@domain.org) are invalid Linux usernames
# and would cause useradd to fail.
def pre_spawn_hook(spawner):
    import os

    username = spawner.user.name
    user_dir = f"/srv/notebooks/{username}"
    shared_link = f"{user_dir}/shared"
    os.makedirs(user_dir, exist_ok=True)
    if not os.path.exists(shared_link):
        os.symlink("/srv/shared_notebooks", shared_link)


c.Spawner.pre_spawn_hook = pre_spawn_hook
