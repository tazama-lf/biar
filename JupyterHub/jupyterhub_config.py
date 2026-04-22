import os
import subprocess

c = get_config()  # noqa: F821

# --- Spawner: local processes, all on the same server ---
c.JupyterHub.spawner_class = "simple"
c.Spawner.notebook_dir = "/srv/notebooks"
c.Spawner.args = ["--ServerApp.root_dir=/srv/notebooks"]
c.Spawner.default_url = "/lab"

# Pass environment variables from JupyterHub to each user's notebook server
c.Spawner.environment = {
    "SPARK_HOME": os.environ.get("SPARK_HOME", "/opt/spark"),
    "JAVA_HOME": os.environ.get("JAVA_HOME", "/opt/java"),
    "SPARK_JARS": os.environ.get("SPARK_JARS", ""),
    "S3A_ENDPOINT": os.environ.get("S3A_ENDPOINT", ""),
    "S3A_ACCESS_KEY": os.environ.get("S3A_ACCESS_KEY", ""),
    "S3A_SECRET_KEY": os.environ.get("S3A_SECRET_KEY", ""),
    "WAREHOUSE_ROOT": os.environ.get("WAREHOUSE_ROOT", "/opt/Tazama_Hudi_warehouse"),
    "SPARK_DRIVER_MEMORY": os.environ.get("SPARK_DRIVER_MEMORY", "4g"),
    "PYSPARK_PYTHON": "python3",
    "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
}

# --- Authentication: NativeAuthenticator with admin signup ---
c.JupyterHub.authenticator_class = "nativeauthenticator.NativeAuthenticator"

# First user to sign up must be the admin (set via JUPYTERHUB_ADMIN env var)
admin = os.environ.get("JUPYTERHUB_ADMIN", "admin")
c.Authenticator.admin_users = {admin}

# New signups require admin approval before they can log in.
# NativeAuthenticator's own is_authorized flag (set to 0 on signup) is the
# security gate — allow_all=True just prevents JupyterHub from adding a second,
# conflicting block on top of NativeAuthenticator's own authorization check.
c.NativeAuthenticator.open_signup = False
c.Authenticator.allow_all = True

# --- Networking ---
c.JupyterHub.ip = "0.0.0.0"
c.JupyterHub.port = 8000

# --- Persistence ---
c.JupyterHub.cookie_secret_file = "/data/jupyterhub_cookie_secret"
c.JupyterHub.db_url = "sqlite:////data/jupyterhub.sqlite"


# Ensure shared notebooks are readable by all spawned servers
def pre_spawn_hook(spawner):
    # SimpleLocalProcessSpawner runs as root — no system user creation needed.
    # Email-style usernames (e.g. user@domain.org) are invalid Linux usernames
    # and would cause useradd to fail. Just fix notebook permissions.
    subprocess.run(["chmod", "-R", "o+rX", "/srv/notebooks"], check=False)


c.Spawner.pre_spawn_hook = pre_spawn_hook
