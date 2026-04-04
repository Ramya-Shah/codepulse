# CodePulse 🚀
**Zero-Config Runtime Observability & Code Heatmapping for Node.js**

CodePulse is a highly-scalable, drop-in application performance monitoring (APM) system designed to detect silent bottlenecks and dead code physically across your architecture without requiring a single `console.log`.

It consists of three decoupled components:
1. **The SDK**: A lightweight, zero-dependency Node interceptor that proxies your functions.
2. **The Ingest Engine**: A Fastify API natively bound to a massive ClickHouse database for ultra-high throughput telemetry ingestion.
3. **The React Dashboard**: A stunning glass-morphic visualizer that pulls your physical directory tree directly from GitHub and merges it with glowing real-time telemetry.

---

## 🌎 How to Deploy CodePulse Publicly (For Everyone to Use)

To take CodePulse out of your local development environment and offer it to your entire company (or the public internet), follow these exact 3 deployment steps:

### Phase 1: Deploy the Backend Engine (ClickHouse + Fastify)
The engine needs a server with a public IP so external servers can ship telemetry to it.

1. Rent an Ubuntu server on AWS EC2, DigitalOcean, or Hetzner.
2. Install Docker & Docker Compose.
3. Copy `docker-compose.yml`, the `codepulse-ingest` folder, and the `codepulse-sdk` folder to the server.
4. Run the cluster:
   ```bash
   docker compose up -d --build
   ```
5. **CRITICAL:** Set up an NGINX reverse-proxy or AWS API Gateway to attach an SSL Certificate (`https://`) to port `3000`. You cannot use standard `http://` in production because web browsers and secure node apps will block the payload.

### Phase 2: Deploy the React Dashboard
Your entire team needs to access the control panel from their browsers.

1. Navigate to the frontend directory: `cd codepulse-dashboard`
2. Update `App.tsx` so the WebSockets and REST API hit your new public server instead of `localhost:3000`.
3. Build the static payload:
   ```bash
   pnpm install
   pnpm build
   ```
4. Upload the generated `dist/` folder to a CDN like **Vercel**, **Netlify**, or AWS S3. 

### Phase 3: Publish the SDK to NPM
Right now, the SDK is a local mono-repo package. You need to push it to the public NPM registry so any developer on earth can type `npm install codepulse-sdk`.

1. Navigate to `cd codepulse-sdk`
2. Ensure you have an NPM account and are logged in (`npm login`).
3. Add a `.npmignore` file to remove tests and local configs.
4. Run:
   ```bash
   npm publish --access public
   ```

---

## 💻 Usage (For Developers using your Platform)

Once your system is public, any developer in the world can instrument their production backend with it!

**1. Install the Package:**
```bash
npm install codepulse-sdk
```

**2. Initialize it at the top of their Entry File:**
*(This MUST be executed before any other `require` or `import` statements!)*

```javascript
// index.js
require('codepulse-sdk').init({
  ingestUrl: 'https://api.your-codepulse.com/ingest', // Your public Phase 1 server
  projectId: 'my-production-backend',
  githubRepo: 'their-username/their-repo'
});

const express = require('express');
// ... the rest of their app runs normally!
```

---

## 🔒 Roadmap & Security Warnings
Before you share your server endpoint (`ingestUrl`) openly on the internet, remember that **Fastify currently allows unauthenticated POSTs**. If a malicious bot finds your ingest URL, they can flood your ClickHouse database.

Before releasing this as a SaaS:
*   Implement API Keys inside Fastify.
*   Require developers to pass an `apiKey: '...'` inside `init({})`.
*   Establish rate-limiting on Fastify using `@fastify/rate-limit`.
