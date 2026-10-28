# codepulse-sdk

Zero-config, function-level performance monitoring for Node.js. One `init()` call times every exported function in your app and streams call counts and latency histograms to a [CodePulse](https://github.com/Ramya-Shah/codepulse) server, whose dashboard highlights the slowest code and can explain the root cause with AI.

**Live demo:** https://codepulse-live.vercel.app/

## Install

```bash
npm install codepulse-sdk
```

## Use

Call `init()` first, before your other `require`s:

```js
require('codepulse-sdk').init({
  ingestUrl: 'https://your-codepulse-server/ingest',
  apiKey: process.env.CODEPULSE_API_KEY,
  projectId: 'my-app',
  githubRepo: 'you/your-repo'
});

const app = require('./app'); // instrumented automatically
```

Run your own server with `docker compose up` from the [CodePulse repo](https://github.com/Ramya-Shah/codepulse).

## Options

| Option | Required | Description |
|---|---|---|
| `ingestUrl` | yes | Your server's `/ingest` URL |
| `projectId` | yes | Groups telemetry for one app |
| `githubRepo` | yes | `owner/repo`, used to link functions to source |
| `apiKey` | | Sent as `x-api-key`; must match the server's `INGEST_API_KEY` |
| `rootDir` | | Directory paths are reported relative to (default `process.cwd()`); set it to your repo root |
| `flushIntervalMs` | | How often batches are sent (default `5000`) |

## How it works

- Hooks Node's module loader and wraps each local CommonJS module's exported functions in a `Proxy` that times every call, sync or async. `node_modules` is skipped.
- Aggregates in memory and sends one batch per interval: per function, the call count, average and a latency histogram (so the server can compute p95).
- Exported functions that are never called are reported too, so dead code shows up.
- No dependencies.

## Limitations

- CommonJS (`require`) only; ES modules are not instrumented.
- Only exported functions are timed, not inner helpers.

## License

MIT
