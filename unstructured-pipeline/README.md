# BIAR Document Processor

A TypeScript-based document processing pipeline that extracts text from documents stored in CouchDB, indexes them in Solr for search, and forwards to NiFi for downstream processing.

## Features

- **Document Extraction**: Fetches documents and attachments from CouchDB
- **Text Extraction**: Uses Apache Tika to extract text and metadata from PDFs, Office documents, and more
- **Full-Text Search**: Indexes extracted content in Apache Solr
- **Data Pipeline**: Sends processed documents to Apache NiFi for further processing
- **Encrypted File Support**: Decrypts AES-256-GCM encrypted attachments
- **Cron Scheduling**: Configurable scheduled processing with node-cron
- **Conflict Resolution**: Automatic retry logic for CouchDB 409 conflicts
- **Content Truncation**: Handles large documents by truncating content for Solr while preserving full text for NiFi

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   CouchDB   │───▶│    Tika     │───▶│    Solr     │    │    NiFi     │
│  (Storage)  │    │ (Extraction)│    │  (Search)   │    │ (Pipeline)  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                                                        ▲
       │                                                        │
       └────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure Environment

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

### 3. Start Infrastructure (Docker)

```bash
docker-compose up -d
```

This starts:

- Apache Tika (port 9998)
- Apache Solr (port 8983)
- Apache NiFi (port 8080/8081)

### 4. Run the Processor

```bash
# Development mode with hot reload
npm run dev

# Production build
npm run build
npm start
```

## Environment Variables

| Variable           | Type    | Description                     | Example                                |
| ------------------ | ------- | ------------------------------- | -------------------------------------- |
| `NODE_ENV`         | string  | Environment mode                | `dev`, `production`                    |
| `FUNCTION_NAME`    | string  | Service identifier for logging  | `biar-document-processor`              |
| `COUCHDB_URL`      | string  | CouchDB database URL with auth  | `http://user:pass@localhost:5984/db`   |
| `TIKA_URL`         | string  | Apache Tika server URL          | `http://localhost:9998`                |
| `SOLR_URL`         | string  | Solr core URL                   | `http://localhost:8983/solr/biar_docs` |
| `NIFI_URL`         | string  | NiFi ListenHTTP endpoint        | `http://localhost:8081`                |
| `CRON_ENABLED`     | boolean | Enable scheduled processing     | `true`                                 |
| `CRON_SCHEDULE`    | string  | Cron expression                 | `*/5 * * * *` (every 5 min)            |
| `MAX_FILE_SIZE_MB` | number  | Maximum file size to process    | `50`                                   |
| `MAX_SOLR_CONTENT` | number  | Max characters to index in Solr | `30000`                                |
| `TIKA_TIMEOUT`     | number  | Tika request timeout (ms)       | `120000`                               |
| `NIFI_TIMEOUT`     | number  | NiFi request timeout (ms)       | `120000`                               |

## Scripts

| Command                 | Description                             |
| ----------------------- | --------------------------------------- |
| `npm run dev`           | Start with hot reload (nodemon)         |
| `npm run build`         | Compile TypeScript to JavaScript        |
| `npm start`             | Run compiled production build           |
| `npm test`              | Run tests with Jest                     |
| `npm run test:coverage` | Run tests with coverage report          |
| `npm run lint`          | Run ESLint and Prettier checks          |
| `npm run fix:eslint`    | Auto-fix ESLint issues                  |
| `npm run fix:prettier`  | Auto-fix formatting issues              |
| `npm run clean`         | Remove build artifacts and node_modules |

## Document Processing Flow

1. **Fetch**: Get unprocessed documents from CouchDB (status != COMPLETED, not archived)
2. **Validate**: Check file size against `MAX_FILE_SIZE_MB` limit
3. **Decrypt**: If file has encryption metadata, decrypt using AES-256-GCM
4. **Extract**: Send file to Tika for text and metadata extraction
5. **Index**: Store extracted content in Solr (truncated to `MAX_SOLR_CONTENT`)
6. **Forward**: Send full content and metadata to NiFi
7. **Update**: Mark document as COMPLETED in CouchDB

## Testing

```bash
# Run all tests
npm test

# Run with coverage
npm run test:coverage

# Watch mode
npm run test:watch
```

Current coverage: **100%** on `job.ts`

## Docker Deployment

Build and run with Docker:

```bash
# Build image
docker build -t biar-processor .

# Run container
docker run -d --env-file .env biar-processor
```

## Project Structure

```
src/
├── config.ts                 # Environment configuration
├── job.ts                    # Main document processor
├── interfaces/
│   ├── iEvidenceDocument.ts  # CouchDB document interface
├── services/
│   ├── CouchDBService.ts     # CouchDB operations
│   ├── DecryptionService.ts  # AES-256-GCM decryption
│   ├── NiFiService.ts        # NiFi HTTP client
│   ├── SolrService.ts        # Solr indexing
│   └── TikaService.ts        # Tika text extraction
└── types/
    └── types.ts              # Shared types
```

## License

MIT
