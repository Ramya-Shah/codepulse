import Fastify, { FastifyReply, FastifyRequest } from 'fastify';
import cors from '@fastify/cors';
import fastifyWebsocket from '@fastify/websocket';
import rateLimit from '@fastify/rate-limit';
import postgres from 'postgres';
import { z } from 'zod';
import { initAnalysisTable, registerAnalysisRoutes } from './analysis';
import { initChatTable, registerChatRoutes } from './chat';

const fastify = Fastify({
  logger: { level: process.env.LOG_LEVEL || 'info' },
  trustProxy: process.env.TRUST_PROXY === 'true',
  bodyLimit: 1024 * 1024
});

// Supabase and other hosted Postgres need TLS; a local container does not.
const sql = postgres(process.env.DATABASE_URL || '', {
  ssl: process.env.DATABASE_SSL === 'false' ? false : { rejectUnauthorized: false },
  max: Number(process.env.DATABASE_POOL_SIZE || 5),
  onnotice: () => {}
});

// Write key for SDKs. CODEPULSE_API_KEY is the old name, still accepted.
const API_KEY = process.env.INGEST_API_KEY || process.env.CODEPULSE_API_KEY || '';

// The one project anyone may read without a key: the live demo.
const DEMO = {
  projectId: process.env.DEMO_PROJECT_ID || 'demo-project',
  repo: process.env.DEMO_REPO || 'Ramya-Shah/CodePulse',
  pathPrefix: process.env.DEMO_PATH_PREFIX || 'demo/'
};

// Must match LATENCY_BUCKETS_MS in codepulse-sdk.
const LATENCY_BUCKETS_MS = [1, 2, 5, 10, 15, 25, 40, 60, 80, 100, 125, 150, 200, 300, 500, 750, 1000, 2500, 5000];
const BUCKET_COUNT = LATENCY_BUCKETS_MS.length + 1;

const RAW_RETENTION_HOURS = Number(process.env.RAW_RETENTION_HOURS || 48);
const ROLLUP_RETENTION_DAYS = Number(process.env.ROLLUP_RETENTION_DAYS || 7);

const ingestSchema = z.array(z.object({
  project_id: z.string().min(1).max(128),
  repo: z.string().min(1).max(256),
  file_path: z.string().min(1).max(512),
  function_name: z.string().min(1).max(256),
  call_count: z.number().int().nonnegative(),
  avg_duration_ms: z.number().nonnegative(),
  // Sent by codepulse-sdk >= 1.1. Older SDKs only send the batch average.
  duration_buckets: z.array(z.number().int().nonnegative()).length(BUCKET_COUNT).optional(),
  timestamp: z.string()
})).max(5000);

const statsQuerySchema = z.object({
  projectId: z.string(),
  repo: z.string(),
  minutes: z.coerce.number().int().min(1).max(7 * 24 * 60).optional(),
  hours: z.coerce.number().int().min(1).max(7 * 24).optional()
});

const projectQuerySchema = z.object({ projectId: z.string() });

// Live call rate per project, pushed to dashboards every second. SDKs flush
// every 5s, so the rate is averaged over that window instead of spiking.
const RATE_WINDOW_SECONDS = 5;
const liveClients = new Map<string, Set<any>>();
const callsThisSecond = new Map<string, number>();
const recentSeconds = new Map<string, number[]>();

function currentRate(projectId: string): number {
  const window = recentSeconds.get(projectId) || [];
  return Math.round(window.reduce((a, b) => a + b, 0) / RATE_WINDOW_SECONDS);
}
let lastIngestAt: string | null = null;

function providedKey(request: FastifyRequest): string | undefined {
  const header = request.headers['x-api-key'];
  return (Array.isArray(header) ? header[0] : header) || (request.query as any)?.apiKey;
}

function hasValidKey(request: FastifyRequest): boolean {
  return !API_KEY || providedKey(request) === API_KEY;
}

// Reads are open for the demo project only; everything else needs the key.
function canRead(request: FastifyRequest, projectId: string): boolean {
  return projectId === DEMO.projectId || hasValidKey(request);
}

function denyUnauthorized(reply: FastifyReply) {
  return reply.status(401).send({ error: 'Unauthorized: Invalid API Key' });
}

function bucketIndex(durationMs: number): number {
  const idx = LATENCY_BUCKETS_MS.findIndex(bound => durationMs <= bound);
  return idx === -1 ? LATENCY_BUCKETS_MS.length : idx;
}

// Estimates a percentile from histogram counts by interpolating inside the bucket.
function percentileFromBuckets(buckets: number[], q: number): number {
  const total = buckets.reduce((a, b) => a + b, 0);
  if (total === 0) return 0;
  const target = q * total;
  let cumulative = 0;
  for (let i = 0; i < buckets.length; i++) {
    if (cumulative + buckets[i] >= target) {
      const lower = i === 0 ? 0 : LATENCY_BUCKETS_MS[i - 1];
      const upper = i < LATENCY_BUCKETS_MS.length ? LATENCY_BUCKETS_MS[i] : LATENCY_BUCKETS_MS[i - 1] * 2;
      return lower + (upper - lower) * ((target - cumulative) / buckets[i]);
    }
    cumulative += buckets[i];
  }
  return LATENCY_BUCKETS_MS[LATENCY_BUCKETS_MS.length - 1];
}

// Per-function totals and histogram-based p95 for the last `minutes`.
export interface FunctionStats {
  file_path: string;
  function_name: string;
  total_calls: number;
  avg_duration: number;
  p95_duration: number;
  last_seen: Date;
}

async function loadStats(projectId: string, repo: string, minutes: number): Promise<FunctionStats[]> {
  const [totals, buckets] = await Promise.all([
    sql`
      SELECT file_path, function_name,
             SUM(call_count)::bigint AS total_calls,
             SUM(total_duration_ms) AS total_ms,
             MAX(minute) AS last_seen
      FROM function_stats_minute
      WHERE project_id = ${projectId} AND repo = ${repo}
        AND minute >= now() - make_interval(mins => ${minutes})
      GROUP BY file_path, function_name
    `,
    sql`
      SELECT file_path, function_name, u.i::int AS i, SUM(u.v)::bigint AS c
      FROM function_stats_minute, unnest(buckets) WITH ORDINALITY AS u(v, i)
      WHERE project_id = ${projectId} AND repo = ${repo}
        AND minute >= now() - make_interval(mins => ${minutes})
      GROUP BY file_path, function_name, u.i
    `
  ]);

  const histograms = new Map<string, number[]>();
  for (const b of buckets) {
    const key = `${b.file_path}\0${b.function_name}`;
    const hist = histograms.get(key) || new Array(BUCKET_COUNT).fill(0);
    hist[b.i - 1] = Number(b.c);
    histograms.set(key, hist);
  }

  const rows: FunctionStats[] = totals.map(t => {
    const totalCalls = Number(t.total_calls);
    const hist = histograms.get(`${t.file_path}\0${t.function_name}`) || new Array(BUCKET_COUNT).fill(0);
    return {
      file_path: t.file_path,
      function_name: t.function_name,
      total_calls: totalCalls,
      avg_duration: totalCalls ? Number(t.total_ms) / totalCalls : 0,
      p95_duration: percentileFromBuckets(hist, 0.95),
      last_seen: t.last_seen
    };
  });
  return rows;
}

// Called after plugins are registered so rate limiting applies to every route.
function registerRoutes() {
  fastify.post('/ingest', async (request, reply) => {
    if (!hasValidKey(request)) return denyUnauthorized(reply);

    const parsed = ingestSchema.safeParse(request.body);
    if (!parsed.success) return reply.status(400).send({ error: 'Invalid payload' });
    const data = parsed.data;
    if (data.length === 0) return { success: true, rows: 0 };

    // Merge rows that share a key so one upsert never touches the same row twice.
    const rollups = new Map<string, { project_id: string; repo: string; file_path: string; function_name: string; call_count: number; total_duration_ms: number; buckets: number[] }>();
    for (const d of data) {
      callsThisSecond.set(d.project_id, (callsThisSecond.get(d.project_id) || 0) + d.call_count);

      const buckets = d.duration_buckets ?? new Array(BUCKET_COUNT).fill(0);
      if (!d.duration_buckets && d.call_count > 0) buckets[bucketIndex(d.avg_duration_ms)] = d.call_count;

      const key = `${d.project_id}\0${d.repo}\0${d.file_path}\0${d.function_name}`;
      const existing = rollups.get(key);
      if (existing) {
        existing.call_count += d.call_count;
        existing.total_duration_ms += d.avg_duration_ms * d.call_count;
        existing.buckets = existing.buckets.map((v, i) => v + buckets[i]);
      } else {
        rollups.set(key, {
          project_id: d.project_id, repo: d.repo, file_path: d.file_path, function_name: d.function_name,
          call_count: d.call_count, total_duration_ms: d.avg_duration_ms * d.call_count, buckets
        });
      }
    }

    try {
      // Raw rows and the per-minute rollup go in one round trip.
      await sql`
        WITH raw AS (
          INSERT INTO function_calls (project_id, repo, file_path, function_name, call_count, avg_duration_ms, duration_buckets, timestamp)
          SELECT project_id, repo, file_path, function_name, call_count, avg_duration_ms, duration_buckets, timestamp::timestamp
          FROM jsonb_to_recordset(${sql.json(data as any)}::jsonb) AS r(
            project_id text, repo text, file_path text, function_name text,
            call_count bigint, avg_duration_ms double precision, duration_buckets bigint[], timestamp text
          )
        )
        INSERT INTO function_stats_minute AS s (project_id, repo, file_path, function_name, minute, call_count, total_duration_ms, buckets)
        SELECT project_id, repo, file_path, function_name, date_trunc('minute', now()), call_count, total_duration_ms, buckets
        FROM jsonb_to_recordset(${sql.json(Array.from(rollups.values()) as any)}::jsonb) AS r(
          project_id text, repo text, file_path text, function_name text,
          call_count bigint, total_duration_ms double precision, buckets bigint[]
        )
        ON CONFLICT (project_id, repo, minute, file_path, function_name) DO UPDATE SET
          call_count = s.call_count + EXCLUDED.call_count,
          total_duration_ms = s.total_duration_ms + EXCLUDED.total_duration_ms,
          buckets = ARRAY(SELECT a + b FROM unnest(s.buckets, EXCLUDED.buckets) AS u(a, b))
      `;
      lastIngestAt = new Date().toISOString();
      return { success: true, rows: data.length };
    } catch (err) {
      request.log.error(err);
      return reply.status(500).send({ error: 'Failed to store telemetry' });
    }
  });

  setInterval(() => {
    const timestamp = new Date().toISOString();
    for (const projectId of new Set([...recentSeconds.keys(), ...callsThisSecond.keys()])) {
      const window = [...(recentSeconds.get(projectId) || []), callsThisSecond.get(projectId) || 0].slice(-RATE_WINDOW_SECONDS);
      if (window.every(c => c === 0)) recentSeconds.delete(projectId);
      else recentSeconds.set(projectId, window);
    }
    for (const [projectId, clients] of liveClients) {
      const message = JSON.stringify({ type: 'rate', calls: currentRate(projectId), timestamp });
      for (const client of clients) {
        if (client.readyState === 1) client.send(message);
      }
    }
    callsThisSecond.clear();
  }, 1000).unref();

  fastify.get('/stats', async (request, reply) => {
    const parsed = statsQuerySchema.safeParse(request.query);
    if (!parsed.success) return reply.status(400).send({ error: 'Invalid query params' });
    const { projectId, repo } = parsed.data;
    if (!canRead(request, projectId)) return denyUnauthorized(reply);

    const minutes = parsed.data.minutes ?? (parsed.data.hours ?? 24) * 60;

    try {
      const rows = await loadStats(projectId, repo, minutes);
      return { data: rows, window_minutes: minutes };
    } catch (err) {
      request.log.error(err);
      return reply.status(500).send({ error: 'Failed to load stats' });
    }
  });

  // Same rate as /live, for browsers where WebSockets are blocked.
  fastify.get('/rate', async (request, reply) => {
    const parsed = projectQuerySchema.safeParse(request.query);
    if (!parsed.success) return reply.status(400).send({ error: 'Invalid query params' });
    if (!canRead(request, parsed.data.projectId)) return denyUnauthorized(reply);
    return { type: 'rate', calls: currentRate(parsed.data.projectId), timestamp: new Date().toISOString() };
  });

  // Public config the dashboard needs to open the demo with no setup.
  fastify.get('/demo', async () => ({ ...DEMO }));

  // Source tree of the demo repo, fetched with the server's token so visitors
  // never need one. Cached so GitHub rate limits are not a concern.
  let treeCache: { at: number; files: string[]; branch: string } | null = null;
  const TREE_CACHE_MS = 10 * 60 * 1000;

  fastify.get('/tree', async (request, reply) => {
    const parsed = projectQuerySchema.safeParse(request.query);
    if (!parsed.success || parsed.data.projectId !== DEMO.projectId) {
      return reply.status(404).send({ error: 'Tree is only served for the demo project' });
    }
    if (treeCache && Date.now() - treeCache.at < TREE_CACHE_MS) return { files: treeCache.files, branch: treeCache.branch };

    const headers: Record<string, string> = { Accept: 'application/vnd.github+json', 'User-Agent': 'codepulse-ingest' };
    if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;

    try {
      const repoRes = await fetch(`https://api.github.com/repos/${DEMO.repo}`, { headers });
      if (!repoRes.ok) throw new Error(`GitHub repo lookup failed: ${repoRes.status}`);
      const branch = (await repoRes.json() as any).default_branch;
      const treeRes = await fetch(`https://api.github.com/repos/${DEMO.repo}/git/trees/${branch}?recursive=1`, { headers });
      if (!treeRes.ok) throw new Error(`GitHub tree fetch failed: ${treeRes.status}`);
      const tree = await treeRes.json() as any;
      const files = (tree.tree as any[])
        .filter(t => t.type === 'blob' && t.path.startsWith(DEMO.pathPrefix) && /\.(c|m)?[jt]s$/.test(t.path))
        .map(t => t.path as string);
      treeCache = { at: Date.now(), files, branch };
      return { files, branch };
    } catch (err) {
      request.log.warn(err);
      // A stale tree beats an empty navigator.
      if (treeCache) return { files: treeCache.files, branch: treeCache.branch };
      return reply.status(502).send({ error: 'Could not fetch repository tree' });
    }
  });

  fastify.get('/health', async (request, reply) => {
    try {
      const [row] = await sql`
        SELECT COALESCE(SUM(call_count), 0)::bigint AS calls
        FROM function_stats_minute
        WHERE project_id = ${DEMO.projectId} AND minute >= now() - interval '5 minutes'
      `;
      const demoCalls = Number(row.calls);
      return { ok: true, db: true, last_ingest_at: lastIngestAt, demo_calls_last_5m: demoCalls, demo_traffic: demoCalls > 0 };
    } catch (err) {
      request.log.error(err);
      return reply.status(503).send({ ok: false, db: false, last_ingest_at: lastIngestAt });
    }
  });

  const aiDeps = { sql, demo: DEMO, loadStats, canRead, hasValidKey, denyUnauthorized };
  registerAnalysisRoutes(fastify, aiDeps);
  registerChatRoutes(fastify, aiDeps);
}

async function initDb() {
  await sql`
    CREATE TABLE IF NOT EXISTS function_calls (
      id          BIGSERIAL PRIMARY KEY,
      project_id  TEXT NOT NULL,
      repo        TEXT NOT NULL,
      file_path   TEXT NOT NULL,
      function_name TEXT NOT NULL,
      call_count  BIGINT NOT NULL,
      avg_duration_ms DOUBLE PRECISION NOT NULL,
      timestamp   TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `;
  await sql`ALTER TABLE function_calls ADD COLUMN IF NOT EXISTS duration_buckets BIGINT[]`;

  // Index for fast lookups by project + repo + time
  await sql`
    CREATE INDEX IF NOT EXISTS idx_fc_project_repo_ts
    ON function_calls (project_id, repo, timestamp DESC)
  `;

  // Pre-aggregated per-minute stats. The dashboard only ever reads this table.
  await sql`
    CREATE TABLE IF NOT EXISTS function_stats_minute (
      project_id    TEXT NOT NULL,
      repo          TEXT NOT NULL,
      file_path     TEXT NOT NULL,
      function_name TEXT NOT NULL,
      minute        TIMESTAMPTZ NOT NULL,
      call_count    BIGINT NOT NULL,
      total_duration_ms DOUBLE PRECISION NOT NULL,
      buckets       BIGINT[] NOT NULL,
      PRIMARY KEY (project_id, repo, minute, file_path, function_name)
    )
  `;

  await initAnalysisTable(sql);
  await initChatTable(sql);
}

async function pruneOldData() {
  try {
    await sql`DELETE FROM function_calls WHERE timestamp < (now() AT TIME ZONE 'UTC') - make_interval(hours => ${RAW_RETENTION_HOURS})`;
    await sql`DELETE FROM function_stats_minute WHERE minute < now() - make_interval(days => ${ROLLUP_RETENTION_DAYS})`;
  } catch (err) {
    fastify.log.error(err, 'Retention cleanup failed');
  }
}

const start = async () => {
  try {
    const origins = process.env.CORS_ORIGIN?.split(',').map(o => o.trim()).filter(Boolean);
    await fastify.register(cors, { origin: origins && origins.length ? origins : true });
    await fastify.register(fastifyWebsocket);
    // Authenticated SDK traffic is not limited; anonymous demo reads are.
    await fastify.register(rateLimit, {
      max: Number(process.env.PUBLIC_RATE_LIMIT_PER_MIN || 120),
      timeWindow: '1 minute',
      allowList: (request) => !!API_KEY && providedKey(request) === API_KEY
    });

    registerRoutes();

    fastify.get('/live', { websocket: true }, (socket: any, request) => {
      const projectId = String((request.query as any)?.projectId || DEMO.projectId);
      if (!canRead(request, projectId)) {
        socket.close(4401, 'Unauthorized');
        return;
      }
      const clients = liveClients.get(projectId) || new Set();
      clients.add(socket);
      liveClients.set(projectId, clients);
      socket.on('close', () => {
        clients.delete(socket);
        if (clients.size === 0) liveClients.delete(projectId);
      });
    });

    fastify.log.info('Initializing Postgres database...');
    await initDb();
    fastify.log.info('Database ready.');

    await pruneOldData();
    setInterval(pruneOldData, 60 * 60 * 1000).unref();

    const port = process.env.PORT ? parseInt(process.env.PORT) : 3000;
    await fastify.listen({ port, host: '0.0.0.0' });
    fastify.log.info(`Server listening on ${port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
