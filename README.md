# Tazama Lakehouse - Docker Build Package

## Quick Start

### Prerequisites

- Docker Engine 20.10+ and Docker Compose 2.0+
- Internet connection (for downloading dependencies and JAR files)
- 16 GB RAM minimum (32 GB recommended)
- 50 GB disk space

### Build the Docker Image

```bash
docker build -t tazama-lakehouse:latest .
```

**Note:** During the build process, the following JAR files will be automatically downloaded from Maven Central:
- Apache Hudi bundle (hudi-spark3.4-bundle_2.12-0.14.1.jar) ~80 MB
- Hadoop AWS (hadoop-aws-3.3.4.jar) ~38 MB
- AWS Java SDK Bundle (aws-java-sdk-bundle-1.12.262.jar) ~286 MB

**Total download size: ~400 MB**

### Run the Container

```bash
docker run -d \
  --name tazama-lakehouse \
  -p 8000:8000 -p 8001:8001 -p 8002:8002 -p 8003:8003 \
  tazama-lakehouse:latest
```

Or using docker-compose:

```bash
docker-compose up -d
```

### Access Services

- **JupyterHub**: http://localhost:8000
- **Arif API**: http://localhost:8001/tables
- **iZyane API**: http://localhost:8002/tables
- **Asim API**: http://localhost:8003

Wait 45 seconds for all services to start.

## Package Contents

- `Dockerfile` - Docker image definition
- `docker-compose.yml` - Container orchestration
- `requirements.txt` - Python dependencies
- `jupyterhub_config.py` - JupyterHub configuration
- `startup.sh` - Container startup script
- `healthcheck.sh` - Container health monitoring
- `notebooks/` - Jupyter notebooks (9 files)
- `api_services/` - FastAPI services (6 files)
- `lib/` - Empty directory (JAR files downloaded during build)
- `Tazama_Hudi_warehouse/` - Data warehouse structure

## System Requirements

- Docker Engine 20.10+
- Docker Compose 2.0+
- 16 GB RAM minimum (32 GB recommended)
- 50 GB disk space

## Build Options

### Custom Image Tag

```bash
docker build -t tazama-lakehouse:v1.0 .
```

### Build with No Cache

```bash
docker build --no-cache -t tazama-lakehouse:latest .
```

### Save Image to TAR

```bash
docker save -o tazama-lakehouse.tar tazama-lakehouse:latest
```

## Support

This package contains all files needed to build the Tazama Lakehouse Docker image.
All notebooks have been validated and S3 configurations are preserved.

**Status:** Production Ready ✅
