import { readFile } from 'fs/promises';
import path from 'path';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type { Sql } from 'postgres';
import { z } from 'zod';
import type { FunctionStats } from './index';

// AI root-cause analysis of the current hotspot, via the Gemini API.
// Results are cached in Postgres so the public demo costs at most one model
// call per DEMO_ANALYSIS_TTL_HOURS, however many people open it.

const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';
const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite';
const DEMO_ANALYSIS_TTL_HOURS = Number(process.env.DEMO_ANALYSIS_TTL_HOURS || 24);
// Local checkout used for the demo's source instead of GitHub, when set.
const DEMO_SOURCE_ROOT = process.env.DEMO_SOURCE_ROOT || '';
const STATS_WINDOW_MINUTES = 15;
const MAX_SOURCE_CHARS = 8000;
const MAX_RELATED_FILES = 3;

export interface AnalysisDeps {
  sql: Sql;
  demo: { projectId: string; repo: string };
  loadStats: (projectId: string, repo: string, minutes: number) => Promise<FunctionStats[]>;
  canRead: (request: FastifyRequest, projectId: string) => boolean;
  hasValidKey: (request: FastifyRequest) => boolean;
  denyUnauthorized: (reply: FastifyReply) => unknown;
}

interface Evidence {
  window_minutes: number;
  hotspot: EvidenceRow;
  related: EvidenceRow[];
  source_files: string[];
}

interface EvidenceRow {
  file_path: string;
  function_name: string;
  calls: number;
  avg_ms: number;
  p95_ms: number;
  time_spent_ms: number;
}

const analysisSchema = z.object({
  headline: z.string(),
  root_cause: z.string(),
  evidence: z.array(z.string()),
  fix: z.string(),
  fixed_code: z.string(),
  expected_impact: z.string(),
  confidence: z.enum(['high', 'medium', 'low'])
});
type Analysis = z.infer<typeof analysisSchema>;

// Gemini's responseSchema uses the OpenAPI subset with upper-case types.
const GEMINI_RESPONSE_SCHEMA = {
  type: 'OBJECT',
  properties: {
    headline: { type: 'STRING', description: 'One sentence naming the bottleneck and its cause.' },
    root_cause: { type: 'STRING', description: 'Two to four sentences explaining why it is slow, tied to the code.' },
    evidence: { type: 'ARRAY', items: { type: 'STRING' }, description: 'Specific numbers from the telemetry that support the diagnosis.' },
    fix: { type: 'STRING', description: 'What to change, in two or three sentences.' },
    fixed_code: { type: 'STRING', description: 'The rewritten function only, as plain JavaScript without markdown fences.' },
    expected_impact: { type: 'STRING', description: 'Estimated latency after the fix, derived from the measured numbers.' },
    confidence: { type: 'STRING', enum: ['high', 'medium', 'low'] }
  },
  required: ['headline', 'root_cause', 'evidence', 'fix', 'fixed_code', 'expected_impact', 'confidence']
};

const timeSpent = (s: FunctionStats) => s.total_calls * s.avg_duration;
const round = (n: number) => Math.round(n * 10) / 10;

function toEvidenceRow(s: FunctionStats): EvidenceRow {
  return {
    file_path: s.file_path,
    function_name: s.function_name,
    calls: s.total_calls,
    avg_ms: round(s.avg_duration),
    p95_ms: round(s.p95_duration),
    time_spent_ms: Math.round(timeSpent(s))
  };
}

function findHotspot(stats: FunctionStats[]): FunctionStats | null {
  const called = stats.filter(s => s.total_calls > 0);
  return called.length ? called.reduce((a, b) => (timeSpent(b) > timeSpent(a) ? b : a)) : null;
}

async function readSource(projectId: string, repo: string, filePath: string, demoProjectId: string): Promise<string | null> {
  try {
    if (projectId === demoProjectId && DEMO_SOURCE_ROOT) {
      const root = path.resolve(DEMO_SOURCE_ROOT);
      const full = path.resolve(root, filePath);
      if (!full.startsWith(root + path.sep)) return null;
      return (await readFile(full, 'utf8')).slice(0, MAX_SOURCE_CHARS);
    }
    const headers: Record<string, string> = { 'User-Agent': 'codepulse-ingest' };
    if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
    const res = await fetch(`https://raw.githubusercontent.com/${repo}/HEAD/${filePath}`, { headers, signal: AbortSignal.timeout(10000) });
    return res.ok ? (await res.text()).slice(0, MAX_SOURCE_CHARS) : null;
  } catch {
    return null;
  }
}

// Relative require() targets of a file, as repo-relative paths.
function requiredFiles(filePath: string, source: string): string[] {
  const dir = path.posix.dirname(filePath);
  const found = new Set<string>();
  for (const match of source.matchAll(/require\(\s*['"](\.{1,2}\/[^'"]+)['"]\s*\)/g)) {
    let target = path.posix.normalize(path.posix.join(dir, match[1]));
    if (!/\.(c|m)?js$/.test(target)) target += '.js';
    found.add(target);
  }
  return [...found];
}

function buildPrompt(evidence: Evidence, sources: Record<string, string>): string {
  const row = (r: EvidenceRow) => `| ${r.file_path} | ${r.function_name} | ${r.calls} | ${r.avg_ms} | ${r.p95_ms} | ${r.time_spent_ms} |`;
  const table = [
    '| file | function | calls | avg ms | p95 ms | time spent ms |',
    '|---|---|---|---|---|---|',
    row(evidence.hotspot),
    ...evidence.related.map(row)
  ].join('\n');
  const code = Object.entries(sources).map(([file, src]) => `--- ${file} ---\n${src}`).join('\n\n');

  return `You are a Node.js performance engineer. A runtime profiler (CodePulse) measured every exported function of an app over the last ${evidence.window_minutes} minutes. Each call is timed separately, including nested calls, so a caller's time includes its callees.

The hotspot (most total time spent) is ${evidence.hotspot.function_name}() in ${evidence.hotspot.file_path}.

Telemetry (hotspot first, then functions in the files it uses):
${table}

Source code:
${code || '(source unavailable)'}

Explain the root cause of the hotspot's latency. Rules:
- Ground every claim in the telemetry or the code above. Quote the actual numbers (calls, avg, p95, call ratios).
- If the code is unavailable or the data does not support a firm conclusion, say so and set confidence to "low".
- Do not give generic advice that is not supported by this code.
- fixed_code must be the corrected function only, using only functions that exist in the source above.`;
}

async function callGemini(prompt: string): Promise<{ analysis: Analysis; usage: unknown }> {
  const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-goog-api-key': GEMINI_API_KEY },
    body: JSON.stringify({
      contents: [{ role: 'user', parts: [{ text: prompt }] }],
      generationConfig: {
        responseMimeType: 'application/json',
        responseSchema: GEMINI_RESPONSE_SCHEMA,
        temperature: 0.2,
        maxOutputTokens: 2048
      }
    }),
    signal: AbortSignal.timeout(45000)
  });
  if (!res.ok) throw new Error(`Gemini API ${res.status}: ${(await res.text()).slice(0, 300)}`);
  const body = await res.json() as any;
  const text = body.candidates?.[0]?.content?.parts?.map((p: any) => p.text || '').join('') || '';
  return { analysis: analysisSchema.parse(JSON.parse(text)), usage: body.usageMetadata };
}

export async function initAnalysisTable(sql: Sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS ai_analyses (
      project_id    TEXT NOT NULL,
      repo          TEXT NOT NULL,
      file_path     TEXT NOT NULL,
      function_name TEXT NOT NULL,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
      model         TEXT NOT NULL,
      evidence      JSONB NOT NULL,
      analysis      JSONB NOT NULL,
      usage         JSONB,
      PRIMARY KEY (project_id, repo, file_path, function_name)
    )
  `;
}

export function registerAnalysisRoutes(app: FastifyInstance, deps: AnalysisDeps) {
  const { sql } = deps;
  const querySchema = z.object({ projectId: z.string().min(1), repo: z.string().min(1) });
  // One generation per hotspot at a time; concurrent requests share it.
  const inFlight = new Map<string, Promise<unknown>>();

  async function cached(projectId: string, repo: string, hotspot: FunctionStats) {
    const [row] = await sql`
      SELECT created_at, model, evidence, analysis FROM ai_analyses
      WHERE project_id = ${projectId} AND repo = ${repo}
        AND file_path = ${hotspot.file_path} AND function_name = ${hotspot.function_name}
    `;
    return row || null;
  }

  async function generate(projectId: string, repo: string, stats: FunctionStats[], hotspot: FunctionStats) {
    const hotSource = await readSource(projectId, repo, hotspot.file_path, deps.demo.projectId);
    const sources: Record<string, string> = {};
    if (hotSource) sources[hotspot.file_path] = hotSource;

    const relatedFiles = hotSource ? requiredFiles(hotspot.file_path, hotSource).slice(0, MAX_RELATED_FILES) : [];
    for (const file of relatedFiles) {
      const src = await readSource(projectId, repo, file, deps.demo.projectId);
      if (src) sources[file] = src;
    }

    const relevantFiles = new Set([hotspot.file_path, ...relatedFiles]);
    const evidence: Evidence = {
      window_minutes: STATS_WINDOW_MINUTES,
      hotspot: toEvidenceRow(hotspot),
      related: stats
        .filter(s => relevantFiles.has(s.file_path) && s !== hotspot && s.total_calls > 0)
        .sort((a, b) => timeSpent(b) - timeSpent(a))
        .slice(0, 12)
        .map(toEvidenceRow),
      source_files: Object.keys(sources)
    };

    const { analysis, usage } = await callGemini(buildPrompt(evidence, sources));
    const [row] = await sql`
      INSERT INTO ai_analyses (project_id, repo, file_path, function_name, created_at, model, evidence, analysis, usage)
      VALUES (${projectId}, ${repo}, ${hotspot.file_path}, ${hotspot.function_name}, now(), ${GEMINI_MODEL},
              ${sql.json(evidence as any)}, ${sql.json(analysis as any)}, ${sql.json((usage ?? null) as any)})
      ON CONFLICT (project_id, repo, file_path, function_name) DO UPDATE SET
        created_at = EXCLUDED.created_at, model = EXCLUDED.model, evidence = EXCLUDED.evidence,
        analysis = EXCLUDED.analysis, usage = EXCLUDED.usage
      RETURNING created_at, model, evidence, analysis
    `;
    return row;
  }

  const shape = (hotspot: FunctionStats, row: any) => row && {
    file_path: hotspot.file_path,
    function_name: hotspot.function_name,
    created_at: row.created_at,
    model: row.model,
    evidence: row.evidence,
    analysis: row.analysis
  };

  // Cached analysis of the current hotspot, or null if none exists yet.
  app.get('/analysis', async (request, reply) => {
    const parsed = querySchema.safeParse(request.query);
    if (!parsed.success) return reply.status(400).send({ error: 'Invalid query params' });
    const { projectId, repo } = parsed.data;
    if (!deps.canRead(request, projectId)) return deps.denyUnauthorized(reply);

    const hotspot = findHotspot(await deps.loadStats(projectId, repo, STATS_WINDOW_MINUTES));
    if (!hotspot) return { configured: !!GEMINI_API_KEY, hotspot: null, result: null };
    return { configured: !!GEMINI_API_KEY, hotspot: hotspot.function_name, result: shape(hotspot, await cached(projectId, repo, hotspot)) };
  });

  // Generates an analysis. For the public demo a fresh cached result is
  // returned instead, so repeated clicks never spend model credits.
  app.post('/analysis', {
    config: { rateLimit: { max: Number(process.env.ANALYSIS_RATE_LIMIT_PER_HOUR || 10), timeWindow: '1 hour' } }
  }, async (request, reply) => {
    const parsed = querySchema.safeParse(request.body);
    if (!parsed.success) return reply.status(400).send({ error: 'Invalid body' });
    const { projectId, repo } = parsed.data;
    const isDemo = projectId === deps.demo.projectId && repo === deps.demo.repo;
    if (!isDemo && !deps.hasValidKey(request)) return deps.denyUnauthorized(reply);
    if (!GEMINI_API_KEY) return reply.status(503).send({ error: 'AI analysis is not configured on this server' });

    const stats = await deps.loadStats(projectId, repo, STATS_WINDOW_MINUTES);
    const hotspot = findHotspot(stats);
    if (!hotspot) return reply.status(409).send({ error: 'No telemetry yet to analyse' });

    const existing = await cached(projectId, repo, hotspot);
    const ageHours = existing ? (Date.now() - new Date(existing.created_at).getTime()) / 3_600_000 : Infinity;
    if (isDemo && ageHours < DEMO_ANALYSIS_TTL_HOURS) return { result: shape(hotspot, existing), cached: true };

    const key = `${projectId}\0${repo}\0${hotspot.file_path}\0${hotspot.function_name}`;
    try {
      let job = inFlight.get(key);
      if (!job) {
        job = generate(projectId, repo, stats, hotspot).finally(() => inFlight.delete(key));
        inFlight.set(key, job);
      }
      return { result: shape(hotspot, await job), cached: false };
    } catch (err) {
      request.log.error(err, 'AI analysis failed');
      if (existing) return { result: shape(hotspot, existing), cached: true, stale: true };
      return reply.status(502).send({ error: 'The AI model did not return a usable analysis. Try again shortly.' });
    }
  });
}
