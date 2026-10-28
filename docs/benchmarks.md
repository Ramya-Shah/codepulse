# Benchmarks

Measured on 2026-10-28. These are local numbers from a laptop, not production
traffic. Nobody uses CodePulse in production yet.

## Setup

- **Machine:** AMD Ryzen 7 8840U (8 cores / 16 threads), 15.3 GB RAM, Windows 11.
- **Stack:** `docker compose` (Docker Desktop, WSL2 VM with 16 CPUs / 7.4 GB):
  Postgres 16, one ingest container (single Node.js process), the demo app.
- **Load generator:** [`scripts/load-test.mjs`](../scripts/load-test.mjs) (autocannon 8),
  run on the same machine, 50 connections, 30 s, after a 5 s warm-up.
- **Payload:** each request is one batch of 17 rows, the shape `codepulse-sdk`
  sends from one process every 5 s (each row = one function's call count,
  average and latency histogram for that interval). Writes need the API key.

## Ingest throughput (`POST /ingest`)

| Scenario | Batches/s | Rows/s | p50 | p97.5 | p99 | Errors |
|---|---|---|---|---|---|---|
| Batches spread over 50 projects | **1,521** | 25,856 | 29 ms | 67 ms | 76 ms | 0 of 45,810 |
| All batches to one project (worst case) | **476** | 8,099 | 39 ms | 485 ms | 529 ms | 0 of 14,325 |

Every batch writes its raw rows and updates the per-minute rollup in one
statement. In the single-project case all requests update the same 17
rollup rows, so they queue on row locks; that is the ceiling.

For scale: one instrumented process sends 0.2 batches/s, so 476 batches/s
for one project is the steady output of about 2,400 processes.

**Nothing was lost.** After run 1, Postgres held 779,620 raw rows against
778,770 counted 2xx responses. The difference is exactly 50 batches × 17 rows,
the requests in flight on the 50 connections when the timer stopped. The
rollup table held the same 77,962,000 calls as the raw rows.

## Query latency (`GET /stats`, what the dashboard calls every 5 s)

After both runs the database held **1,027,427 raw rows**, representing
**102,924,982 function calls**.

| Query | Data behind it | p50 | p97.5 | p99 |
|---|---|---|---|---|
| `/stats` for one load-test project (15 min window) | 259,981 rows / 25,998,100 calls | 2 ms | 4 ms | 6 ms |
| `/stats` for the demo project | live demo traffic | 4 ms | 6 ms | 8 ms |
| Same per-function p95 computed from raw rows (no rollups) | 259,981 rows | 332 ms (single run) | | |

`/stats` reads the pre-aggregated per-minute table, so its cost depends on
functions × minutes, not on how many events arrived.

p95 values shown in the dashboard are estimated from latency histograms
(19 buckets, interpolated), not exact percentiles.

## What these numbers support

- About **1,500 batched payloads/s (about 26,000 rows/s)** on one ingest process
  and a local Postgres, when load is spread across projects; about 480/s
  when it all goes to one project. "Thousands per second" is not supported.
- p95 queries in **single-digit milliseconds** over **~1 million stored rows
  (~100 million function calls)**.

## Reproduce

```bash
docker compose up -d --build
node scripts/load-test.mjs http://localhost:3000 <INGEST_API_KEY> 30 50 50
```
