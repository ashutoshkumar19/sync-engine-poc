# Bi-Directional Record Synchronization Engine

A synchronization platform built with NestJS to keep records aligned between System A (PostgreSQL) and System B (CRM) with conflict resolution, echo prevention, and rate-limited retries.

---

## Prerequisites

- **Node.js**: v18+ (tested on Node 20+)
- **npm**: v9+
- **Docker & Docker Compose**: for running local PostgreSQL, Redis, and Kafka

---

## Quickstart

### 1. Start Infrastructure Services

Spin up PostgreSQL, Redis, Kafka, and Kafka UI in Docker:

```bash
docker-compose up -d
```

This starts:

- **PostgreSQL** on `localhost:5432` (db: `syncdb`, user/pass: `postgres`/`postgres`)
- **Redis** on `localhost:6379`
- **Kafka** on `localhost:9092`
- **Kafka UI** on `http://localhost:8090`

Database schemas and initial tables are automatically applied from `scripts/init-db.sql`.

### 2. Install Dependencies & Build

```bash
npm install
npm run build
```

### 3. Create Kafka Topics

Run the setup script once to create required topics with desired partition counts:

```bash
npm run setup:kafka
```

---

## Running the Services

The application is split into the **main API server** and **three independent consumers**. Each runs as its own standalone process on a distinct port, meaning you can start and scale them individually.

Open separate terminal tabs for each service:

### Terminal 1: Main API Server (Port 3000)

Handles demo endpoints, manual emit APIs, scenario runners, and the background job scheduler.

```bash
npm run start:main:dev
```

- **Swagger UI**: [http://localhost:3000/docs](http://localhost:3000/docs)
- **Status check**: `curl http://localhost:3000/demo/status`

### Terminal 2: Normalization Consumer (Port 4001)

Listens for incoming raw events (CDC from System A and Webhooks from System B), transforms them into a canonical format, and emits them to `sync-events`.

```bash
npm run start:consumer:normalization
```

- **Swagger / Health**: [http://localhost:4001/docs](http://localhost:4001/docs)

### Terminal 3: Schema Mapping Consumer (Port 5001)

Consumes canonical events, filters out echoed writes, resolves field conflicts via Last-Write-Wins (LWW) in `sync_metadata`, and stages outbound payloads into `sync_events`.

```bash
npm run start:consumer:schema-mapping
```

- **Swagger / Health**: [http://localhost:5001/docs](http://localhost:5001/docs)

### Terminal 4: Sync Consumer (Port 6001)

Listens for scheduled dispatch jobs, batch-claims `PENDING` records from `sync_events`, verifies them against latest metadata, applies rate-limits / circuit breaker rules, and pushes changes to the destination system.

```bash
npm run start:consumer:sync
```

- **Swagger / Health**: [http://localhost:6001/docs](http://localhost:6001/docs)

---

## Production / Single-Command Production Start

To build and run compiled outputs:

```bash
npm run build
npm run start:prod
```

For consumers in production, pass the compiled entry points directly:

```bash
PORT=4001 node dist/src/consumers/normalization/normalization.main.js
PORT=5001 node dist/src/consumers/schema-mapping/schema-mapping.main.js
PORT=6001 node dist/src/consumers/sync/sync.main.js
```

---

## Testing

Run the full Jest test suite:

```bash
npm test
```

Or run test watch mode while developing:

```bash
npm run test:watch
```

An offline test runner that uses in-memory mock storage (no Docker needed) is also available:

```bash
npm run test:local
```

---

## Useful Endpoints (Swagger /docs)

Interactive Swagger documentation is available at `http://localhost:3000/docs`. Common endpoints include:

### Event Emission

- **Emit System A event**: `POST http://localhost:3000/demo/emit/system-a`
- **Emit System B event**: `POST http://localhost:3000/demo/emit/system-b`
- **Emit both concurrently**: `POST http://localhost:3000/demo/emit/both`
- **Emit parallel burst**: `POST http://localhost:3000/demo/emit-burst?count=10&system_a=true&system_b=true`

### Scheduler Controls

- **Start periodic scheduler**: `POST http://localhost:3000/demo/scheduler/start` (optional body: `{"intervalMs": 10000}`)
- **Stop periodic scheduler**: `POST http://localhost:3000/demo/scheduler/stop`
- **Trigger single dispatch cycle**: `POST http://localhost:3000/demo/scheduler/trigger?pipeline_id=pipeline-1`

### Inspection & Health

- **Check staged sync events**: `GET http://localhost:3000/demo/sync-events?status=PENDING`
- **View record metadata**: `GET http://localhost:3000/demo/sync-metadata/:record_id`
- **Fetch System A record**: `GET http://localhost:3000/demo/records/system-a/:id`
- **Fetch System B record**: `GET http://localhost:3000/demo/records/system-b/:id`
- **System health & status**: `GET http://localhost:3000/demo/status`

### Automated Test Scenarios

- **Run scenario**: `POST http://localhost:3000/demo/scenarios/:scenarioName`
  - Scenarios: `independent-concurrent`, `field-conflict-stale`, `echo-suppression-a-to-b`, `rate-limiting-429`, `circuit-breaker`
