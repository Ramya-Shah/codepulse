import Fastify from 'fastify';
import cors from '@fastify/cors';
import fastifyWebsocket from '@fastify/websocket';
import postgres from 'postgres';
import { z } from 'zod';

const fastify = Fastify({ logger: true });

const sql = postgres(process.env.DATABASE_URL || '', {
  ssl: { rejectUnauthorized: false }
});

const ingestSchema = z.array(z.object({
  project_id: z.string(),
  repo: z.string(),
  file_path: z.string(),
  function_name: z.string(),
  call_count: z.number(),
  avg_duration_ms: z.number(),
  timestamp: z.string()
}));

const statsQuerySchema = z.object({
  projectId: z.string(),
  repo: z.string(),
  hours: z.string().optional().default('24')
});

const connectedClients = new Set<any>();
let callsPerSecond = 0;

fastify.post('/ingest', async (request, reply) => {
  try {
    const data = ingestSchema.parse(request.body);
    if (data.length === 0) return { success: true };

    const totalCalls = data.reduce((sum, item) => sum + item.call_count, 0);
    callsPerSecond += totalCalls;

    // Bulk insert using postgres.js
    await sql`
      INSERT INTO function_calls ${sql(data.map(d => ({
        project_id: d.project_id,
        repo: d.repo,
        file_path: d.file_path,
        function_name: d.function_name,
        call_count: d.call_count,
        avg_duration_ms: d.avg_duration_ms,
        timestamp: d.timestamp
      })))}
    `;

    return { success: true };
  } catch (err) {
    fastify.log.error(err);
    reply.status(400).send({ error: 'Invalid payload' });
  }
});

setInterval(() => {
  for (const client of connectedClients) {
    if (client.readyState === 1) {
      client.send(JSON.stringify({
        type: 'rate',
        calls: callsPerSecond,
        timestamp: new Date().toISOString()
      }));
    }
  }
  callsPerSecond = 0;
}, 1000);

fastify.get('/stats', async (request, reply) => {
  try {
    const { projectId, repo, hours } = statsQuerySchema.parse(request.query);
    const hoursInt = parseInt(hours, 10);

    const rows = await sql`
      SELECT
        file_path,
        function_name,
        SUM(call_count) AS total_calls,
        AVG(avg_duration_ms) AS avg_duration,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY avg_duration_ms) AS p95_duration
      FROM function_calls
      WHERE project_id = ${projectId}
        AND repo = ${repo}
        AND timestamp >= NOW() - INTERVAL '1 hour' * ${hoursInt}
      GROUP BY file_path, function_name
    `;

    return { data: rows };
  } catch (err) {
    fastify.log.error(err);
    reply.status(400).send({ error: 'Invalid query params' });
  }
});

fastify.delete('/purge-bad-paths', async (request, reply) => {
  try {
    await sql`
      DELETE FROM function_calls
      WHERE file_path NOT LIKE 'demo/%'
    `;
    return { success: true, message: 'Purged rows with bad file paths' };
  } catch (err) {
    fastify.log.error(err);
    reply.status(500).send({ error: 'Purge failed' });
  }
});

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

  // Index for fast lookups by project + repo + time
  await sql`
    CREATE INDEX IF NOT EXISTS idx_fc_project_repo_ts
    ON function_calls (project_id, repo, timestamp DESC)
  `;
}

const start = async () => {
  try {
    await fastify.register(cors, { origin: true });
    await fastify.register(fastifyWebsocket);

    fastify.addHook('preHandler', (request, reply, done) => {
      if (request.method === 'OPTIONS') return done();

      const expectedKey = process.env.CODEPULSE_API_KEY;
      if (!expectedKey) return done();

      const providedKey = request.headers['x-api-key'] || (request.query as any)?.apiKey;
      if (providedKey !== expectedKey) {
        reply.status(401).send({ error: 'Unauthorized: Invalid API Key' });
        return;
      }
      done();
    });

    fastify.get('/live', { websocket: true }, (connection: any, req) => {
      connectedClients.add(connection);
      connection.on('close', () => {
        connectedClients.delete(connection);
      });
    });

    fastify.log.info('Initializing Postgres database...');
    await initDb();
    fastify.log.info('Database ready.');

    const port = process.env.PORT ? parseInt(process.env.PORT) : 3000;
    await fastify.listen({ port, host: '0.0.0.0' });
    fastify.log.info(`Server listening on ${port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
