import Fastify from 'fastify';
import cors from '@fastify/cors';
import fastifyWebsocket from '@fastify/websocket';
import { createClient } from '@clickhouse/client';
import { z } from 'zod';

const fastify = Fastify({ logger: true });

const clickhouse = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
  database: process.env.CLICKHOUSE_DB || 'default',
});

// columns: project_id, repo, file_path, function_name, call_count, avg_duration_ms, timestamp
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
    // Add to buffer for the 1-second pulse
    callsPerSecond += totalCalls;

    await clickhouse.insert({
      table: 'function_calls',
      values: data,
      format: 'JSONEachRow'
    });
    
    return { success: true };
  } catch (err) {
    fastify.log.error(err);
    reply.status(400).send({ error: 'Invalid payload' });
  }
});

setInterval(() => {
  for (const client of connectedClients) {
    // 1 === WebSocket.OPEN
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
    const query = `
      SELECT 
        file_path, 
        function_name, 
        sum(call_count) as total_calls, 
        avg(avg_duration_ms) as avg_duration,
        quantile(0.95)(avg_duration_ms) as p95_duration
      FROM function_calls
      WHERE project_id = {projectId: String}
        AND repo = {repo: String}
        AND timestamp >= now() - INTERVAL {hours: UInt32} HOUR
      GROUP BY file_path, function_name
    `;

    const result = await clickhouse.query({
      query,
      query_params: { projectId, repo, hours: parseInt(hours, 10) },
      format: 'JSONEachRow'
    });

    const rows = await result.json();
    return { data: rows };
  } catch (err) {
    fastify.log.error(err);
    reply.status(400).send({ error: 'Invalid query params' });
  }
});

async function initDb() {
  await clickhouse.command({
    query: `
      CREATE TABLE IF NOT EXISTS function_calls (
        project_id String,
        repo String,
        file_path String,
        function_name String,
        call_count UInt64,
        avg_duration_ms Float64,
        timestamp DateTime
      ) ENGINE = MergeTree()
      ORDER BY (project_id, repo, file_path, timestamp)
    `
  });
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

    let retries = 30;
    while(retries) {
      try {
        await initDb();
        break;
      } catch(e) {
        retries -= 1;
        fastify.log.warn(`Clickhouse not ready, retrying. ${retries} left`);
        await new Promise(r => setTimeout(r, 2000));
        if (retries === 0) throw e;
      }
    }

    const port = process.env.PORT ? parseInt(process.env.PORT) : 3000;
    await fastify.listen({ port, host: '0.0.0.0' });
    fastify.log.info(`Server listening on ${port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
