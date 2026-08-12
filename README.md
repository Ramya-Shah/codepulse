# CodePulse 🚀

**Zero-Config Runtime Observability & Code Heatmapping for Node.js**

CodePulse is a scalable, drop-in application performance monitoring (APM) platform for Node.js applications. It automatically captures runtime telemetry, identifies performance bottlenecks, and visualizes code-level activity without requiring manual instrumentation or scattered `console.log` statements.

The platform is built around three decoupled components:

1. **SDK** — A lightweight, zero-dependency Node.js interceptor that uses module-load interception and ES6 Proxies to automatically instrument application code.
2. **Ingest Engine** — A Fastify-based telemetry ingestion service backed by ClickHouse for high-throughput storage and analytical queries.
3. **React Dashboard** — A real-time observability dashboard that combines repository structure with runtime telemetry to visualize application activity and performance.

---

## Architecture

```text
┌─────────────────────┐
│     Node.js App     │
│                     │
│    CodePulse SDK    │
└──────────┬──────────┘
           │
           │ Telemetry
           ▼
┌─────────────────────┐
│  Fastify Ingest     │
│      Engine         │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│     ClickHouse      │
│   MergeTree Tables  │
└──────────┬──────────┘
           │
           │ REST / WebSocket
           ▼
┌─────────────────────┐
│   React Dashboard   │
│                     │
│ Runtime + Code View │
└─────────────────────┘
```

---

## Key Features

### Zero-Config Runtime Instrumentation

The CodePulse SDK intercepts Node.js module loading and uses ES6 Proxies to instrument functions at runtime.

```javascript
require('codepulse-sdk').init({
  ingestUrl: 'https://api.your-codepulse.com/ingest',
  projectId: 'my-production-backend',
  githubRepo: 'username/repository'
});
```

No changes are required across the application's existing business logic.

### High-Throughput Telemetry Ingestion

The ingestion engine is built with **Fastify** and **Zod** and is designed to handle high-volume batched telemetry payloads.

Telemetry is persisted in **ClickHouse MergeTree** tables, allowing efficient analytical queries across millions of events.

### Runtime Performance Analytics

CodePulse captures execution information that can be used to analyze:

* Function execution frequency
* Execution latency
* p95 latency
* Runtime hotspots
* Frequently executed code paths
* Potentially unused or low-activity code

### Real-Time Dashboard

The React dashboard provides a live view of application telemetry through WebSockets.

It combines runtime telemetry with repository structure to provide a code-oriented view of application performance.

---

## Tech Stack

| Component               | Technology          |
| ----------------------- | ------------------- |
| SDK                     | Node.js, TypeScript |
| Ingest Engine           | Fastify, Zod        |
| Database                | ClickHouse          |
| Dashboard               | React               |
| Real-Time Communication | WebSockets          |
| Containerization        | Docker              |
| Backend Deployment      | Railway / Docker    |
| Frontend Deployment     | Vercel              |

---

## Project Structure

```text
CodePulse/
├── codepulse-sdk/
│   └── Runtime telemetry SDK
│
├── codepulse-ingest/
│   └── Fastify ingestion service
│
├── codepulse-dashboard/
│   └── React observability dashboard
│
└── docker-compose.yml
```

---

# Local Development

### 1. Clone the repository

```bash
git clone https://github.com/Ramya-Shah/CodePulse.git
cd CodePulse
```

### 2. Start the backend infrastructure

```bash
docker compose up -d --build
```

This starts the required backend services and ClickHouse instance.

### 3. Start the dashboard

```bash
cd codepulse-dashboard
pnpm install
pnpm dev
```

The dashboard can then be accessed through the local development server.

---

# Using the SDK

Initialize CodePulse before loading the rest of the application.

```javascript
require('codepulse-sdk').init({
  ingestUrl: 'http://localhost:3000/ingest',
  projectId: 'my-node-app',
  githubRepo: 'username/repository'
});

const express = require('express');

const app = express();
// Existing application code continues normally.
```

The SDK automatically intercepts supported modules and collects runtime telemetry.

---

# Public Deployment

CodePulse can be deployed as a shared observability platform for multiple applications.

## Phase 1 — Deploy the Ingest Engine

The ingestion service requires a publicly accessible server.

Suitable infrastructure includes:

* AWS EC2
* DigitalOcean
* Hetzner
* Railway
* Other Docker-compatible infrastructure

For a Docker-based deployment:

```bash
docker compose up -d --build
```

For production deployments, place the ingestion service behind HTTPS using a reverse proxy or managed gateway.

Example architecture:

```text
Node.js Applications
        │
        │ HTTPS
        ▼
   Reverse Proxy
        │
        ▼
  Fastify Ingest API
        │
        ▼
    ClickHouse
```

---

## Phase 2 — Deploy the Dashboard

Build the React dashboard:

```bash
cd codepulse-dashboard

pnpm install
pnpm build
```

Deploy the generated `dist/` directory to a static hosting provider such as Vercel, Netlify, or AWS S3.

Configure the dashboard to point to the public ingestion API and WebSocket endpoint.

---

## Phase 3 — Publish the SDK

The SDK can be distributed through the npm registry.

```bash
cd codepulse-sdk
npm login
npm publish --access public
```

Once published, developers can install it using:

```bash
npm install codepulse-sdk
```

---

# Production Security

The current ingestion endpoint is intended for development and controlled deployments.

Before exposing the ingestion API publicly, authentication and abuse protection should be implemented.

### Recommended protections

* API-key authentication
* Request validation
* Rate limiting
* Payload size limits
* Project-level authorization
* HTTPS-only communication
* Telemetry retention policies

For example:

```javascript
require('codepulse-sdk').init({
  ingestUrl: 'https://api.your-codepulse.com/ingest',
  apiKey: 'your-api-key',
  projectId: 'my-production-backend',
  githubRepo: 'username/repository'
});
```

These protections are required before using the system as a production SaaS deployment.

---

## Links

**GitHub:** https://github.com/Ramya-Shah/CodePulse

**Live Dashboard:** https://codepulse-codepulse-dashboard.vercel.app/

---

## License

This project is currently under active development.
