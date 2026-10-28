import type { FastifyInstance } from 'fastify';
import type { Sql } from 'postgres';
import { z } from 'zod';
import {
  GEMINI_API_KEY, GEMINI_MODEL, findHotspot, readSource, round, timeSpent, type AnalysisDeps
} from './analysis';

// "Ask about the code" chat: answers questions about the app's functions,
// grounded in the last 15 minutes of telemetry and the source files.

const STATS_WINDOW_MINUTES = 15;
const MAX_FUNCTIONS_IN_CONTEXT = 25;
const MAX_SOURCE_FILES = 8;
const MAX_TURNS = 8;
const MAX_MESSAGE_CHARS = 600;
// Cap on model calls per day across all visitors, so the public demo can't
// run up a bill. Resets at midnight UTC.
const CHAT_DAILY_LIMIT = Number(process.env.CHAT_DAILY_LIMIT || 500);
// First questions in the demo are answered from a cache, so suggested
// questions are instant and repeat questions cost nothing.
const CHAT_CACHE_HOURS = Number(process.env.CHAT_CACHE_HOURS || 6);
// Models tried in order; the next one is used when a model is overloaded
// (429/5xx) or slower than the per-attempt timeout.
const CHAT_MODELS = (process.env.GEMINI_CHAT_MODELS || `${GEMINI_MODEL},gemini-3.6-flash`).split(',').map(m => m.trim()).filter(Boolean);
const ATTEMPT_TIMEOUT_MS = 30000;

export const SUGGESTED_QUESTIONS = [
  'Why is getOrderHistory slow?',
  'What does getProductById do, and how often is it called?',
  'Which functions are never called?',
  'How would I fix the slowest function?'
];

const normalize = (q: string) => q.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();

export async function initChatTable(sql: Sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS chat_cache (
      project_id TEXT NOT NULL,
      repo       TEXT NOT NULL,
      question   TEXT NOT NULL,
      answer     TEXT NOT NULL,
      model      TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (project_id, repo, question)
    )
  `;
}

const chatSchema = z.object({
  projectId: z.string().min(1),
  repo: z.string().min(1),
  messages: z.array(z.object({
    role: z.enum(['user', 'model']),
    text: z.string().min(1).max(MAX_MESSAGE_CHARS)
  })).min(1).max(MAX_TURNS)
});

let dailyCount = 0;
let dailyKey = new Date().toISOString().slice(0, 10);

function takeDailySlot(): boolean {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== dailyKey) {
    dailyKey = today;
    dailyCount = 0;
  }
  if (dailyCount >= CHAT_DAILY_LIMIT) return false;
  dailyCount++;
  return true;
}

type ChatMessage = { role: 'user' | 'model'; text: string };

// Builds the grounded prompt and asks Gemini. Caches the answer when the
// conversation is a single demo question.
async function answer(deps: AnalysisDeps, projectId: string, repo: string, messages: ChatMessage[], cacheKey: string | null): Promise<{ text: string; model: string }> {
  const stats = await deps.loadStats(projectId, repo, STATS_WINDOW_MINUTES);
  const hotspot = findHotspot(stats);
  const ranked = [...stats].sort((a, b) => timeSpent(b) - timeSpent(a)).slice(0, MAX_FUNCTIONS_IN_CONTEXT);

  const files = [...new Set(ranked.map(s => s.file_path))].slice(0, MAX_SOURCE_FILES);
  const sources = (await Promise.all(files.map(async f => [f, await readSource(projectId, repo, f, deps.demo.projectId)] as const)))
    .filter(([, src]) => src)
    .map(([f, src]) => `--- ${f} ---\n${src}`)
    .join('\n\n');

  const perHotspot = (calls: number) => hotspot && hotspot.total_calls ? round(calls / hotspot.total_calls) : 0;
  const table = [
    `| file | function | calls | ${hotspot ? `calls per ${hotspot.function_name}() call` : 'calls per hotspot call'} | avg ms | p95 ms | time spent ms |`,
    '|---|---|---|---|---|---|---|',
    ...ranked.map(s => `| ${s.file_path} | ${s.function_name} | ${s.total_calls} | ${perHotspot(s.total_calls)} | ${round(s.avg_duration)} | ${round(s.p95_duration)} | ${Math.round(timeSpent(s))} |`)
  ].join('\n');

  const systemInstruction = `You are the assistant inside CodePulse, a performance monitor for Node.js. You answer questions about one app's functions: what they do, how often they run, and why some are slow.

You have the last ${STATS_WINDOW_MINUTES} minutes of measured telemetry and the app's source code. Each exported function call is timed separately, so a caller's time includes its callees. "calls" = 0 means the function exists but was never called. The file column is where a function is defined, not who calls it.
${hotspot ? `The function with the most total time spent is ${hotspot.function_name}() in ${hotspot.file_path}.` : 'No telemetry has arrived yet.'}

Telemetry:
${table}

Source code:
${sources || '(unavailable)'}

Rules:
- Ground answers in the telemetry and code above and quote the real numbers. Use the precomputed columns instead of doing arithmetic.
- If the data doesn't answer the question, say so. Never invent measurements or functions.
- Keep answers under 120 words, in plain sentences. Use short code snippets only when asked how to fix something.
- Only discuss this app, its performance and CodePulse. Politely decline anything else.`;

  const request = JSON.stringify({
    systemInstruction: { parts: [{ text: systemInstruction }] },
    contents: messages.map(m => ({ role: m.role, parts: [{ text: m.text }] })),
    generationConfig: { temperature: 0.3, maxOutputTokens: 600 }
  });

  let text = '';
  let model = '';
  let lastError: unknown = null;
  for (const candidate of CHAT_MODELS) {
    try {
      const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${candidate}:generateContent`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-goog-api-key': GEMINI_API_KEY },
        body: request,
        signal: AbortSignal.timeout(ATTEMPT_TIMEOUT_MS)
      });
      if (!res.ok) throw new Error(`Gemini API ${candidate} ${res.status}: ${(await res.text()).slice(0, 200)}`);
      const body = await res.json() as any;
      text = (body.candidates?.[0]?.content?.parts || []).map((p: any) => p.text || '').join('').trim();
      if (!text) throw new Error(`Gemini API ${candidate}: empty answer`);
      model = candidate;
      break;
    } catch (err) {
      lastError = err;
    }
  }
  if (!text) throw lastError;

  if (cacheKey) {
    await deps.sql`
      INSERT INTO chat_cache (project_id, repo, question, answer, model)
      VALUES (${projectId}, ${repo}, ${cacheKey}, ${text}, ${model})
      ON CONFLICT (project_id, repo, question) DO UPDATE SET
        answer = EXCLUDED.answer, model = EXCLUDED.model, created_at = now()
    `;
  }
  return { text, model };
}

// Answers the suggested questions ahead of time and keeps them fresh, so
// clicking one is instant even when the model is slow.
function warmSuggestions(app: FastifyInstance, deps: AnalysisDeps) {
  if (!GEMINI_API_KEY) return;
  const run = async () => {
    for (const q of SUGGESTED_QUESTIONS) {
      try {
        await answer(deps, deps.demo.projectId, deps.demo.repo, [{ role: 'user', text: q }], normalize(q));
      } catch (err) {
        app.log.warn(err, `Could not pre-answer "${q}"`);
      }
    }
  };
  // Wait for some telemetry first, then refresh just before the cache expires.
  setTimeout(run, 2 * 60 * 1000).unref();
  setInterval(run, Math.max(1, CHAT_CACHE_HOURS - 0.5) * 3_600_000).unref();
}

export function registerChatRoutes(app: FastifyInstance, deps: AnalysisDeps) {
  app.get('/chat/suggestions', async () => ({ questions: SUGGESTED_QUESTIONS }));
  warmSuggestions(app, deps);

  app.post('/chat', {
    config: { rateLimit: { max: Number(process.env.CHAT_RATE_LIMIT_PER_10_MIN || 12), timeWindow: '10 minutes' } }
  }, async (request, reply) => {
    const parsed = chatSchema.safeParse(request.body);
    if (!parsed.success) return reply.status(400).send({ error: `Messages must be under ${MAX_MESSAGE_CHARS} characters.` });
    const { projectId, repo, messages } = parsed.data;
    const isDemo = projectId === deps.demo.projectId && repo === deps.demo.repo;
    if (!isDemo && !deps.hasValidKey(request)) return deps.denyUnauthorized(reply);
    if (messages[messages.length - 1].role !== 'user') return reply.status(400).send({ error: 'Last message must be a question.' });

    // Only a conversation's first question is cacheable; follow-ups depend on history.
    const cacheKey = isDemo && messages.length === 1 ? normalize(messages[0].text) : null;
    if (cacheKey) {
      const [hit] = await deps.sql`
        SELECT answer, model, created_at FROM chat_cache
        WHERE project_id = ${projectId} AND repo = ${repo} AND question = ${cacheKey}
          AND created_at > now() - make_interval(hours => ${CHAT_CACHE_HOURS})
      `;
      if (hit) return { answer: hit.answer, model: hit.model, cached_at: hit.created_at };
    }

    if (!GEMINI_API_KEY) return reply.status(503).send({ error: 'The chat is not configured on this server.' });
    if (!takeDailySlot()) return reply.status(429).send({ error: 'The demo chat has reached its daily limit. Please try again tomorrow.' });

    try {
      const result = await answer(deps, projectId, repo, messages, cacheKey);
      return { answer: result.text, model: result.model };
    } catch (err) {
      request.log.error(err, 'Chat failed');
      return reply.status(502).send({ error: 'The AI model is slow to respond right now. Please try again.' });
    }
  });
}
