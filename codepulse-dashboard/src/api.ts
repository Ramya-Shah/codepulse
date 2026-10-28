export interface FunctionStat {
  file_path: string;
  function_name: string;
  total_calls: number;
  avg_duration: number;
  p95_duration: number;
}

export interface DemoConfig {
  projectId: string;
  repo: string;
  pathPrefix: string;
}

// Settings for "connect your own project". Nothing here is needed for the demo.
export interface CustomProject {
  ingestUrl: string;
  apiKey: string;
  repo: string;
  projectId: string;
}

// Accepts "host", "https://host" or ".../ingest" and returns "https://host".
export function normalizeBaseUrl(url: string): string {
  let base = url.trim().replace(/\/+$/, '').replace(/\/ingest$/, '');
  if (!/^https?:\/\//.test(base)) {
    base = (/^(localhost|127\.0\.0\.1)(:|$)/.test(base) ? 'http://' : 'https://') + base;
  }
  return base;
}

export const DEFAULT_API_URL = normalizeBaseUrl(import.meta.env.VITE_API_URL || 'http://localhost:3000');

export function liveSocketUrl(baseUrl: string, projectId: string, apiKey?: string): string {
  const params = new URLSearchParams({ projectId });
  if (apiKey) params.set('apiKey', apiKey);
  return `${baseUrl.replace(/^http/, 'ws')}/live?${params}`;
}

async function getJson<T>(url: string, apiKey?: string): Promise<T> {
  const res = await fetch(url, { headers: apiKey ? { 'x-api-key': apiKey } : {} });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export const fetchDemoConfig = (baseUrl: string) => getJson<DemoConfig>(`${baseUrl}/demo`);

export async function fetchDemoTree(baseUrl: string, projectId: string): Promise<string[]> {
  const data = await getJson<{ files: string[] }>(`${baseUrl}/tree?projectId=${encodeURIComponent(projectId)}`);
  return data.files;
}

// Public repos only: the browser calls GitHub directly, without a token.
export async function fetchPublicGithubTree(repo: string): Promise<string[]> {
  const repoData = await getJson<{ default_branch: string }>(`https://api.github.com/repos/${repo}`);
  const tree = await getJson<{ tree: { path: string; type: string }[] }>(
    `https://api.github.com/repos/${repo}/git/trees/${repoData.default_branch}?recursive=1`
  );
  return tree.tree.filter(t => t.type === 'blob' && /\.(c|m)?[jt]s$/.test(t.path)).map(t => t.path);
}

export async function fetchStats(baseUrl: string, projectId: string, repo: string, minutes: number, apiKey?: string): Promise<FunctionStat[]> {
  const params = new URLSearchParams({ projectId, repo, minutes: String(minutes) });
  const data = await getJson<{ data: FunctionStat[] }>(`${baseUrl}/stats?${params}`, apiKey);
  return data.data.map(s => ({
    ...s,
    file_path: s.file_path.replace(/\\/g, '/'),
    total_calls: Number(s.total_calls),
    avg_duration: Number(s.avg_duration),
    p95_duration: Number(s.p95_duration)
  }));
}

export type Heat = 'slow' | 'medium' | 'fast' | 'dead' | 'none';

export function functionHeat(stat: FunctionStat): Heat {
  if (stat.total_calls === 0) return 'dead';
  if (stat.avg_duration > 100) return 'slow';
  if (stat.avg_duration > 20) return 'medium';
  return 'fast';
}

// A file is as hot as its slowest called function.
export function fileHeat(stats: FunctionStat[]): Heat {
  if (stats.length === 0) return 'none';
  const order: Heat[] = ['slow', 'medium', 'fast', 'dead'];
  const heats = stats.map(functionHeat);
  return order.find(h => heats.includes(h)) || 'none';
}

// Time spent = calls x average latency. This is what ranks a hotspot, so a
// slow-but-rare call does not outrank a moderately slow one on every request.
export const timeSpent = (s: FunctionStat) => s.total_calls * s.avg_duration;

export interface AnalysisEvidenceRow {
  file_path: string;
  function_name: string;
  calls: number;
  avg_ms: number;
  p95_ms: number;
  time_spent_ms: number;
}

export interface AnalysisResult {
  file_path: string;
  function_name: string;
  created_at: string;
  model: string;
  evidence: {
    window_minutes: number;
    hotspot: AnalysisEvidenceRow;
    related: AnalysisEvidenceRow[];
    source_files: string[];
  };
  analysis: {
    headline: string;
    root_cause: string;
    evidence: string[];
    fix: string;
    fixed_code: string;
    expected_impact: string;
    confidence: 'high' | 'medium' | 'low';
  };
}

export async function fetchAnalysis(baseUrl: string, projectId: string, repo: string, apiKey?: string) {
  const params = new URLSearchParams({ projectId, repo });
  return getJson<{ configured: boolean; hotspot: string | null; result: AnalysisResult | null }>(`${baseUrl}/analysis?${params}`, apiKey);
}

export async function requestAnalysis(baseUrl: string, projectId: string, repo: string, apiKey?: string): Promise<AnalysisResult> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (apiKey) headers['x-api-key'] = apiKey;
  const res = await fetch(`${baseUrl}/analysis`, { method: 'POST', headers, body: JSON.stringify({ projectId, repo }) });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error || `Request failed (${res.status})`);
  return body.result;
}
