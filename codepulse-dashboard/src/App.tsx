import { useState, useEffect, useMemo, useRef, useCallback, type ChangeEvent } from 'react';
import { Activity, FolderTree, Code2, ExternalLink, Flame, GitBranch, Package, Settings2, X } from 'lucide-react';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer
} from 'recharts';
import {
  DEFAULT_API_URL, fetchDemoConfig, fetchDemoTree, fetchPublicGithubTree, fetchStats,
  fileHeat, functionHeat, liveSocketUrl, normalizeBaseUrl, timeSpent,
  type CustomProject, type FunctionStat
} from './api';
import './App.css';

const REPO_URL = 'https://github.com/Ramya-Shah/CodePulse';
const NPM_URL = 'https://www.npmjs.com/package/codepulse-sdk';
const STATS_WINDOW_MINUTES = 15;
const STATS_POLL_MS = 5000;

interface ChartDataPoint {
  time: string;
  calls: number;
}

type LinkStatus = 'connecting' | 'live' | 'reconnecting' | 'unreachable';

// The project being viewed: the public demo, or one the visitor connected.
interface Target {
  baseUrl: string;
  projectId: string;
  repo: string;
  apiKey?: string;
  isDemo: boolean;
}

function loadCustomProject(): CustomProject | null {
  try {
    const raw = localStorage.getItem('customProject');
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function saveCustomProject(project: CustomProject | null) {
  try {
    if (project) localStorage.setItem('customProject', JSON.stringify(project));
    else localStorage.removeItem('customProject');
  } catch {
    // Storage can be unavailable (private mode); the session still works.
  }
}

// Streams the per-second call rate and reconnects with backoff when dropped.
function useLiveRate(target: Target | null) {
  const [chartData, setChartData] = useState<ChartDataPoint[]>([]);
  const [socketOpen, setSocketOpen] = useState(false);

  useEffect(() => {
    if (!target) return;
    let ws: WebSocket | null = null;
    let retryTimer: number | undefined;
    let attempt = 0;
    let stopped = false;

    const connect = () => {
      ws = new WebSocket(liveSocketUrl(target.baseUrl, target.projectId, target.apiKey));
      ws.onopen = () => {
        attempt = 0;
        setSocketOpen(true);
      };
      ws.onclose = () => {
        setSocketOpen(false);
        if (stopped) return;
        retryTimer = window.setTimeout(connect, Math.min(1000 * 2 ** attempt++, 15000));
      };
      ws.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          if (payload.type !== 'rate') return;
          setChartData(prev => {
            const next = [...prev, { time: new Date(payload.timestamp).toLocaleTimeString(), calls: payload.calls }];
            return next.length > 60 ? next.slice(-60) : next;
          });
        } catch {
          // ignore malformed frames
        }
      };
    };

    connect();
    return () => {
      stopped = true;
      window.clearTimeout(retryTimer);
      ws?.close();
    };
  }, [target]);

  return { chartData, socketOpen };
}

// Polls the aggregated per-function stats for the current window.
function useStats(target: Target | null) {
  const [stats, setStats] = useState<FunctionStat[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [failures, setFailures] = useState(0);

  useEffect(() => {
    if (!target) return;
    let cancelled = false;

    const load = async () => {
      try {
        const rows = await fetchStats(target.baseUrl, target.projectId, target.repo, STATS_WINDOW_MINUTES, target.apiKey);
        if (cancelled) return;
        setStats(rows);
        setLoaded(true);
        setFailures(0);
      } catch {
        if (!cancelled) setFailures(f => f + 1);
      }
    };

    load();
    const id = window.setInterval(load, STATS_POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [target]);

  return { stats, loaded, failures };
}

function formatMs(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${ms.toFixed(1)}ms`;
}

function App() {
  const [customProject, setCustomProject] = useState<CustomProject | null>(loadCustomProject);
  const [demoTarget, setDemoTarget] = useState<Target | null>(null);
  const [demoError, setDemoError] = useState<string | null>(null);

  // Ask the backend which project the public demo shows, retrying until it answers.
  useEffect(() => {
    let cancelled = false;
    const resolve = async () => {
      let attempt = 0;
      while (!cancelled) {
        try {
          const config = await fetchDemoConfig(DEFAULT_API_URL);
          if (cancelled) return;
          setDemoTarget({ baseUrl: DEFAULT_API_URL, projectId: config.projectId, repo: config.repo, isDemo: true });
          setDemoError(null);
          return;
        } catch {
          attempt++;
          if (attempt >= 3 && !cancelled) setDemoError('The live demo backend is not responding. Retrying…');
          await new Promise(r => setTimeout(r, Math.min(2000 * attempt, 10000)));
        }
      }
    };
    resolve();
    return () => { cancelled = true; };
  }, []);

  const target = useMemo<Target | null>(() => customProject ? {
    baseUrl: normalizeBaseUrl(customProject.ingestUrl),
    projectId: customProject.projectId,
    repo: customProject.repo,
    apiKey: customProject.apiKey || undefined,
    isDemo: false
  } : demoTarget, [customProject, demoTarget]);

  const connectProject = (project: CustomProject | null) => {
    saveCustomProject(project);
    setCustomProject(project);
  };

  // Keyed so switching projects starts every panel from a clean state.
  return (
    <Dashboard
      key={target ? `${target.baseUrl}|${target.projectId}|${target.repo}` : 'pending'}
      target={target}
      setupError={customProject ? null : demoError}
      customProject={customProject}
      onConnect={connectProject}
    />
  );
}

function Dashboard({ target, setupError, customProject, onConnect }: {
  target: Target | null;
  setupError: string | null;
  customProject: CustomProject | null;
  onConnect: (project: CustomProject | null) => void;
}) {
  const [tree, setTree] = useState<string[]>([]);
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [showConnect, setShowConnect] = useState(false);

  // Demo files come from the server; a visitor's own repo is read from GitHub directly.
  useEffect(() => {
    if (!target) return;
    let cancelled = false;
    const load = target.isDemo ? fetchDemoTree(target.baseUrl, target.projectId) : fetchPublicGithubTree(target.repo);
    load.then(files => !cancelled && setTree(files)).catch(() => {});
    return () => { cancelled = true; };
  }, [target]);

  const { stats, loaded, failures } = useStats(target);
  const { chartData, socketOpen } = useLiveRate(target);

  const status: LinkStatus = setupError || failures >= 3
    ? 'unreachable'
    : !target || !loaded
      ? 'connecting'
      : socketOpen ? 'live' : 'reconnecting';

  // Files from the repo plus any file telemetry mentions that the tree lacks.
  const files = useMemo(() => {
    const all = new Set(tree);
    stats.forEach(s => all.add(s.file_path));
    return Array.from(all).sort();
  }, [tree, stats]);

  const statsByFile = useMemo(() => {
    const map = new Map<string, FunctionStat[]>();
    for (const s of stats) map.set(s.file_path, [...(map.get(s.file_path) || []), s]);
    return map;
  }, [stats]);

  const hotspot = useMemo(() => {
    const called = stats.filter(s => s.total_calls > 0);
    return called.length ? called.reduce((a, b) => (timeSpent(b) > timeSpent(a) ? b : a)) : null;
  }, [stats]);

  const tableRows = useMemo(() => {
    const rows = selectedFile ? statsByFile.get(selectedFile) || [] : stats;
    return [...rows].sort((a, b) => timeSpent(b) - timeSpent(a));
  }, [selectedFile, stats, statsByFile]);

  const sourceLink = useCallback((filePath: string) => target ? `https://github.com/${target.repo}/blob/HEAD/${filePath}` : '#', [target]);

  const connectProject = (project: CustomProject | null) => {
    setShowConnect(false);
    onConnect(project);
  };

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo"><Flame size={18} /></span>
          <div>
            <h1>CodePulse</h1>
            <p className="tagline">Zero-config performance monitoring for Node.js: one line of setup, and it shows which functions are slow.</p>
          </div>
        </div>
        <nav className="topbar-links">
          <StatusPill status={status} isDemo={target?.isDemo ?? !customProject} />
          <a href={REPO_URL} target="_blank" rel="noreferrer"><GitBranch size={16} /> GitHub</a>
          <a href={NPM_URL} target="_blank" rel="noreferrer"><Package size={16} /> npm</a>
          <button className="btn-ghost" onClick={() => setShowConnect(true)}><Settings2 size={16} /> Your app</button>
        </nav>
      </header>

      {customProject ? (
        <div className="notice">
          Viewing your project <strong>{customProject.projectId}</strong>.
          <button className="link-button" onClick={() => connectProject(null)}>Back to the live demo</button>
        </div>
      ) : (
        <ol className="steps">
          <li><span>1</span><p>A sample Node.js store app runs with <code>codepulse-sdk</code> loaded. That's the only change to its code.</p></li>
          <li><span>2</span><p>Every 5 seconds the SDK sends call counts and latency for each function it sees.</p></li>
          <li><span>3</span><p>This page ranks functions by total time spent and flags the hotspot. Click a file to drill in.</p></li>
        </ol>
      )}

      {setupError && <div className="notice warn">{setupError}</div>}

      <section className="hotspot panel">
        <div className="panel-header"><Flame size={18} color="var(--accent)" /> Hottest function right now</div>
        {hotspot ? (
          <div className="hotspot-body">
            <div className="hotspot-name">
              <code>{hotspot.function_name}()</code>
              <a href={sourceLink(hotspot.file_path)} target="_blank" rel="noreferrer" className="file-link">
                {hotspot.file_path} <ExternalLink size={12} />
              </a>
            </div>
            <dl className="hotspot-metrics">
              <div><dt>Calls ({STATS_WINDOW_MINUTES} min)</dt><dd>{hotspot.total_calls.toLocaleString()}</dd></div>
              <div><dt>Avg latency</dt><dd className={`heat-text ${functionHeat(hotspot)}`}>{formatMs(hotspot.avg_duration)}</dd></div>
              <div><dt>p95 latency</dt><dd>{formatMs(hotspot.p95_duration)}</dd></div>
              <div><dt>Time spent</dt><dd>{formatMs(timeSpent(hotspot))}</dd></div>
            </dl>
          </div>
        ) : (
          <div className="placeholder">
            {status === 'unreachable' ? 'Waiting for the backend…' : loaded ? 'Waiting for the first batch of telemetry…' : <span className="loading-dots">Loading live data</span>}
          </div>
        )}
      </section>

      <div className="grid">
        <section className="panel files-panel">
          <div className="panel-header">
            <FolderTree size={18} color="var(--accent)" /> Files
            {selectedFile && <button className="link-button right" onClick={() => setSelectedFile(null)}>Show all</button>}
          </div>
          <div className="file-tree">
            {files.length === 0 ? (
              <div className="placeholder">{status === 'unreachable' ? 'No connection yet.' : <span className="loading-dots">Loading files</span>}</div>
            ) : (
              files.map(path => (
                <button
                  key={path}
                  className={`file-item ${selectedFile === path ? 'selected' : ''}`}
                  onClick={() => setSelectedFile(selectedFile === path ? null : path)}
                  title={path}
                >
                  <span className={`heat-dot ${fileHeat(statsByFile.get(path) || [])}`} />
                  <span className="file-path">{path}</span>
                </button>
              ))
            )}
          </div>
          <div className="legend">
            <span><i className="heat-dot slow" /> &gt;100ms</span>
            <span><i className="heat-dot medium" /> 20–100ms</span>
            <span><i className="heat-dot fast" /> fast</span>
            <span><i className="heat-dot dead" /> never called</span>
          </div>
        </section>

        <section className="panel table-panel">
          <div className="panel-header">
            <Code2 size={18} color="var(--accent)" />
            Functions {selectedFile ? <span className="muted">in {selectedFile.split('/').pop()}</span> : <span className="muted">by time spent, last {STATS_WINDOW_MINUTES} min</span>}
          </div>
          <div className="table-scroll">
            {tableRows.length === 0 ? (
              <div className="placeholder">
                {selectedFile ? 'No telemetry for this file yet.' : status === 'unreachable' ? 'No connection yet.' : <span className="loading-dots">Loading telemetry</span>}
              </div>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>Function</th>
                    <th className="num">Calls</th>
                    <th className="num">Avg</th>
                    <th className="num">p95</th>
                    <th className="hide-sm">Source</th>
                  </tr>
                </thead>
                <tbody>
                  {tableRows.map(stat => {
                    const heat = functionHeat(stat);
                    const isHot = hotspot && stat.file_path === hotspot.file_path && stat.function_name === hotspot.function_name;
                    return (
                      <tr key={`${stat.file_path}::${stat.function_name}`} className={isHot ? 'hot-row' : ''}>
                        <td className="td-func">
                          {stat.function_name}()
                          {!selectedFile && <div className="td-file">{stat.file_path}</div>}
                        </td>
                        <td className="num">{stat.total_calls.toLocaleString()}</td>
                        <td className="num"><span className={`metric-badge ${heat}`}>{heat === 'dead' ? 'unused' : formatMs(stat.avg_duration)}</span></td>
                        <td className="num muted">{stat.total_calls ? formatMs(stat.p95_duration) : '–'}</td>
                        <td className="hide-sm">
                          <a href={sourceLink(stat.file_path)} target="_blank" rel="noreferrer" className="github-link">
                            Code <ExternalLink size={12} />
                          </a>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        </section>
      </div>

      <section className="panel chart-panel">
        <div className="panel-header"><Activity size={18} color="var(--accent)" /> Function calls per second (live)</div>
        <div className="chart-wrapper">
          {chartData.length === 0 ? (
            <div className="placeholder">{status === 'unreachable' ? 'No connection yet.' : <span className="loading-dots">Waiting for the live stream</span>}</div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="colorCalls" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="var(--accent)" stopOpacity={0.6} />
                    <stop offset="95%" stopColor="var(--accent)" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="4 4" stroke="#fde8d8" vertical={false} />
                <XAxis dataKey="time" stroke="#b08080" fontSize={11} tickLine={false} axisLine={false} dy={10} minTickGap={40} />
                <YAxis stroke="#b08080" fontSize={11} tickLine={false} axisLine={false} dx={-6} width={44} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#fff', borderColor: '#fde8d8', borderRadius: '12px', boxShadow: '0 4px 20px rgba(80,20,20,0.1)' }}
                  itemStyle={{ color: '#2d1515', fontWeight: 600 }}
                  labelStyle={{ color: '#b08080', fontSize: '0.85rem' }}
                />
                <Area type="monotone" dataKey="calls" stroke="#E36A6A" strokeWidth={2.5} fillOpacity={1} fill="url(#colorCalls)" isAnimationActive={false} />
              </AreaChart>
            </ResponsiveContainer>
          )}
        </div>
      </section>

      {showConnect && (
        <ConnectDialog
          initial={customProject}
          onClose={() => setShowConnect(false)}
          onConnect={connectProject}
        />
      )}
    </div>
  );
}

function StatusPill({ status, isDemo }: { status: LinkStatus; isDemo: boolean }) {
  const label = {
    connecting: 'Connecting…',
    live: isDemo ? 'Live demo' : 'Live',
    reconnecting: 'Reconnecting…',
    unreachable: 'Backend offline, retrying'
  }[status];
  return <span className={`status-pill ${status}`}><i />{label}</span>;
}

function ConnectDialog({ initial, onClose, onConnect }: {
  initial: CustomProject | null;
  onClose: () => void;
  onConnect: (project: CustomProject | null) => void;
}) {
  const [form, setForm] = useState<CustomProject>(initial || { ingestUrl: '', apiKey: '', repo: '', projectId: '' });
  const set = (key: keyof CustomProject) => (e: ChangeEvent<HTMLInputElement>) => setForm({ ...form, [key]: e.target.value });
  const canSubmit = form.ingestUrl && form.repo.includes('/') && form.projectId;
  const firstInput = useRef<HTMLInputElement>(null);

  useEffect(() => {
    firstInput.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  return (
    <div className="dialog-backdrop" onClick={onClose}>
      <div className="dialog" role="dialog" aria-modal="true" aria-labelledby="connect-title" onClick={e => e.stopPropagation()}>
        <button className="dialog-close" onClick={onClose} aria-label="Close"><X size={18} /></button>
        <h2 id="connect-title">Use CodePulse in your own app</h2>
        <ol className="quickstart">
          <li>Run the ingest server (<code>docker compose up</code> in the repo).</li>
          <li><code>npm install codepulse-sdk</code></li>
          <li>Call <code>init()</code> before your other <code>require</code>s:</li>
        </ol>
        <pre>{`require('codepulse-sdk').init({
  ingestUrl: 'https://your-server/ingest',
  apiKey: process.env.CODEPULSE_API_KEY,
  projectId: 'my-app',
  githubRepo: 'you/your-repo'
});`}</pre>
        <h3>Then view it here</h3>
        <form onSubmit={e => { e.preventDefault(); if (canSubmit) onConnect(form); }}>
          <label>Ingest server URL<input ref={firstInput} value={form.ingestUrl} onChange={set('ingestUrl')} placeholder="https://your-server" /></label>
          <label>API key<input type="password" value={form.apiKey} onChange={set('apiKey')} placeholder="The server's INGEST_API_KEY" autoComplete="off" /></label>
          <label>GitHub repo (public)<input value={form.repo} onChange={set('repo')} placeholder="you/your-repo" /></label>
          <label>Project ID<input value={form.projectId} onChange={set('projectId')} placeholder="my-app" /></label>
          <p className="fineprint">These settings stay in this browser only.</p>
          <div className="dialog-actions">
            {initial && <button type="button" className="btn-ghost" onClick={() => onConnect(null)}>Back to demo</button>}
            <button type="submit" className="btn-primary" disabled={!canSubmit}>Connect</button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default App;
