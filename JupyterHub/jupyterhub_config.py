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

    from tornado.httpclient import HTTPClientError

    # Set by get_token_info when the refresh-token exchange fails for a reason
    # that says nothing about the token (Keycloak down, network error, 5xx).
    # Each hub request runs in its own asyncio task, so this is per-refresh.
    _refresh_transient_error = contextvars.ContextVar(
        "refresh_transient_error", default=False
    )

    class KeycloakOAuthenticator(GenericOAuthenticator):
        """GenericOAuthenticator that ends the session when Keycloak revokes it.

        When Keycloak rejects the refresh token (session timeout, admin logout,
        password change), the stock refresh_user only returns False. The user's
        single-user server keeps running and keeps calling the hub, and every
        call retries the dead refresh token, forever (tazama-lf/biar#209).
        Here we stop the user's servers and clear the hub cookie instead, so
        the next visit goes through a fresh Keycloak login.
        """

        async def get_token_info(self, handler, params):
            try:
                return await super().get_token_info(handler, params)
            except HTTPClientError as e:
                # 4xx (e.g. 400 invalid_grant) means the token itself was
                # rejected; anything else (599 connection error, 5xx) is
                # Keycloak being unavailable.
                if not 400 <= e.code < 500:
                    _refresh_transient_error.set(True)
                raise
            except OSError:
                _refresh_transient_error.set(True)
                raise

        async def refresh_user(self, user, handler=None, **kwargs):
            _refresh_transient_error.set(False)
            result = await super().refresh_user(user, handler=handler, **kwargs)
            if result is not False:
                return result
            if _refresh_transient_error.get():
                # Don't kill running notebooks over a Keycloak outage; the
                # request is still denied and the next refresh retries.
                self.log.warning(
                    f"Keycloak unreachable refreshing {user.name}; "
                    "leaving servers running"
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
