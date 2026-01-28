#!/bin/bash
# Health check script for Tazama Docker container

set -e

# Check if Spark Master is responding
if ! curl -sf http://localhost:8080 > /dev/null 2>&1; then
    echo "Spark Master UI not responding"
    exit 1
fi

# Check if at least one FastAPI service is responding
if ! curl -sf http://localhost:8001/docs > /dev/null 2>&1 && \
   ! curl -sf http://localhost:8002/docs > /dev/null 2>&1 && \
   ! curl -sf http://localhost:8003/docs > /dev/null 2>&1; then
    echo "No FastAPI services responding"
    exit 1
fi

# Check if JupyterHub is responding
if ! curl -sf http://localhost:8000/hub/api > /dev/null 2>&1; then
    echo "JupyterHub not responding"
    exit 1
fi

# All checks passed
exit 0
