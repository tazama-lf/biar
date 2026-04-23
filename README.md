# BIAR — Business Intelligence & Analytics Repository

BIAR is a data lakehouse and document processing platform that provides end-to-end capabilities for data ingestion, ETL processing, storage, querying, and interactive analysis. It is designed to work within the Tazama fraud management ecosystem.

## Architecture

The platform consists of the following components:

| Component                   | Description                                                                                                          | Port |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------- | ---- |
| **automation-orchestrator** | PySpark-based ETL orchestrator with FastAPI endpoints for triggering data lake pipelines using Apache Hudi           | 7619 |
| **datalakehouse-api**       | FastAPI query service with thread-safe Spark session management for querying the data lakehouse                      | 8282 |
| **JupyterHub**              | Multi-user Jupyter notebook environment with pre-built fraud analytics dashboards                                    | 8888 |
| **nifi**                    | Apache NiFi 1.24.0 instance for data flow orchestration and routing                                                  | 8088 |
| **unstructured-pipeline**   | TypeScript service that extracts, decrypts, and indexes unstructured documents (PDFs, Office docs) via Tika and Solr | —    |

### Supporting Services (via Docker Compose)

| Service              | Port(s)                  | Purpose                           |
| -------------------- | ------------------------ | --------------------------------- |
| Apache Tika          | 9998                     | Document text/metadata extraction |
| Apache Solr          | 8983                     | Full-text search and indexing     |
| Apache Ozone SCM     | 9876 (UI), 9860 (client) | Storage Container Manager         |
| Apache Ozone OM      | 9874 (UI), 9862 (RPC)    | Object Manager                    |
| Ozone Datanodes (x3) | —                        | Distributed storage               |
| Ozone Recon          | 9888                     | Cluster monitoring                |
| Ozone S3 Gateway     | 9878                     | S3-compatible object access       |
| aws-cli              | —                        | S3 bucket initialization          |

## Technology Stack

- **Languages:** Python 3.9, TypeScript (Node.js), Java
- **Big Data:** Apache Spark 3.4.2, Apache Hadoop 3, Apache Hudi 0.14.1
- **Storage:** Apache Ozone 2.0.0, CouchDB
- **Search:** Apache Solr 9
- **Orchestration:** Apache NiFi 1.24.0
- **APIs:** FastAPI, Uvicorn
- **Notebooks:** JupyterHub / JupyterLab

## Prerequisites

- Docker and Docker Compose
- Sufficient memory for Spark workloads (recommended 8 GB+ available to Docker)

## Getting Started

1. **Configure environment variables**

   Review and update `docker-config` with your Ozone cluster settings (endpoints, storage paths, replication).

2. **Start the platform**

   ```bash
   docker compose up -d
   ```

   This brings up all services including Ozone, Tika, Solr, NiFi, the ETL orchestrator, query API, and JupyterHub.

3. **Access the services**
   - JupyterHub: [http://localhost:8888](http://localhost:8888)
   - NiFi: [http://localhost:8088](http://localhost:8088)
   - Datalakehouse API: [http://localhost:8282](http://localhost:8282)
   - Automation Orchestrator API: [http://localhost:7619](http://localhost:7619)
   - Solr Admin: [http://localhost:8983](http://localhost:8983)
   - Ozone Recon: [http://localhost:9888](http://localhost:9888)

## Components

### Automation Orchestrator

A FastAPI service backed by PySpark that manages ETL job execution. It provides REST endpoints to trigger Spark jobs that read, transform, and write data using the Apache Hudi lakehouse format. Supports multi-worker job queues and Hadoop AWS (S3A) connectivity.

### Datalakehouse API

A query API built with FastAPI that exposes the data lakehouse for ad-hoc querying. It manages a thread-safe Spark session with automatic recovery and handles concurrent queries via a thread pool.

### JupyterHub

A multi-user notebook environment pre-loaded with analytics dashboards:

- Case Management Trend Dashboard
- Case Tracking Analysis Dashboard
- Dashboard Metrics
- Executive Overview Dashboard
- Fraud Trend Analysis Dashboard
- Fraud Typology Effectiveness Dashboard
- TMS Performance Dashboard

### NiFi

A custom Apache NiFi deployment with pre-built flow templates (`tazama.xml`) and PostgreSQL JDBC driver. Initialization scripts automatically configure parameter contexts for S3, Ozone, and HTTP endpoints.
https://github.com/tazama-lf/docs/blob/dev/Technical/Deployment-Guides/Nifi-Deployment-Guide.md

### Unstructured Pipeline

A TypeScript cron-based job that processes unstructured documents:

1. Fetches unprocessed documents from CouchDB
2. Decrypts AES-256-GCM encrypted attachments
3. Extracts text and metadata via Apache Tika
4. Indexes content in Solr (truncated to 30 KB for search)
5. Forwards full content to NiFi for downstream processing

Supports files up to 50 MB with automatic retry and status tracking.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
