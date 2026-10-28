// Load test for POST /ingest with SDK-shaped batches.
//
//   node scripts/load-test.mjs [apiUrl] [apiKey] [seconds] [connections] [projects]
//
// Each request is one batch of ROWS_PER_BATCH rows, the shape codepulse-sdk
// sends every 5s from one process. Requests rotate across `projects`
// project IDs (loadtest-0, loadtest-1, ...) so the demo project is untouched.
import autocannon from 'autocannon';

const [apiUrl = 'http://localhost:3000', apiKey = 'local-dev-key', seconds = '30', connections = '50', projects = '1'] = process.argv.slice(2);
const ROWS_PER_BATCH = 17;
const BUCKET_COUNT = 20; // must match LATENCY_BUCKETS_MS.length + 1 in the SDK

function batch(projectId) {
  const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19);
  return JSON.stringify(Array.from({ length: ROWS_PER_BATCH }, (_, i) => {
    const buckets = new Array(BUCKET_COUNT).fill(0);
    const calls = 20 + i * 10;
    buckets[3 + (i % 8)] = calls;
    return {
      project_id: projectId,
      repo: 'loadtest/repo',
      file_path: `src/services/service${i % 6}.js`,
      function_name: `handler${i}`,
      call_count: calls,
      avg_duration_ms: 2 + i * 3,
      duration_buckets: buckets,
      timestamp
    };
  }));
}

let n = 0;
const result = await autocannon({
  url: `${apiUrl}/ingest`,
  method: 'POST',
  connections: Number(connections),
  duration: Number(seconds),
  headers: { 'content-type': 'application/json', 'x-api-key': apiKey },
  setupClient: (client) => {
    client.on('response', () => {});
  },
  requests: [{
    setupRequest: (req) => ({ ...req, body: batch(`loadtest-${n++ % Number(projects)}`) })
  }]
});

const ok = result['2xx'];
const durationS = result.duration;
console.log(JSON.stringify({
  seconds: durationS,
  connections: Number(connections),
  projects: Number(projects),
  rows_per_batch: ROWS_PER_BATCH,
  requests_total: result.requests.total,
  requests_2xx: ok,
  requests_non_2xx: result.non2xx,
  errors: result.errors,
  timeouts: result.timeouts,
  batches_per_sec_avg: Math.round(ok / durationS),
  rows_per_sec_avg: Math.round((ok * ROWS_PER_BATCH) / durationS),
  latency_ms: { p50: result.latency.p50, p97_5: result.latency.p97_5, p99: result.latency.p99, max: result.latency.max }
}, null, 2));
