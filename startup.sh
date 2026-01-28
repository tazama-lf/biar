#!/bin/bash
set -e

echo "=========================================="
echo "Starting Tazama Services"
echo "=========================================="

# Set up environment variables for Java
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Wait a bit for all services to be ready
sleep 2

echo "Starting FastAPI for Arif on port 8001..."
nohup python3 /app/api_services/run_arif_api.py > /var/log/fastapi_arif.log 2>&1 &
echo $! > /var/run/fastapi_arif.pid

echo "Starting FastAPI for iZyane on port 8002..."
nohup python3 /app/api_services/run_izyane_api.py > /var/log/fastapi_izyane.log 2>&1 &
echo $! > /var/run/fastapi_izyane.pid

echo "Starting Asim Test API on port 8003..."
nohup python3 /app/api_services/run_asim_api.py > /var/log/fastapi_asim.log 2>&1 &
echo $! > /var/run/fastapi_asim.pid

sleep 5

echo "=========================================="
echo "Starting JupyterHub"
echo "=========================================="
# Create necessary directories for JupyterHub
mkdir -p /srv/jupyterhub
mkdir -p /srv/notebook

# Create symlinks in notebook directory for JARs so notebooks can find them
cd /app/notebooks
ln -sf /app/lib/*.jar .
ln -sf /app/Tazama_Hudi_warehouse .

# Start JupyterHub in foreground (keeps container running)
exec jupyterhub --config=/app/jupyterhub_config.py
