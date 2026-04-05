import { useState, useEffect, useMemo, useRef } from 'react';
import { Activity, FolderTree, Code2, ExternalLink } from 'lucide-react';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer
} from 'recharts';
import './App.css';

interface FileStat {
  file_path: string;
  function_name: string;
  total_calls: number;
  avg_duration: number;
  p95_duration: number;
}

interface GithubTreeItem {
  path: string;
  type: string;
}

interface ChartDataPoint {
  time: string;
  calls: number;
}

function App() {
  const [token, setToken] = useState(localStorage.getItem('ghToken') || '');
  const [repo, setRepo] = useState(localStorage.getItem('targetRepo') || 'codepulse/demo');
  const [projectId, setProjectId] = useState(localStorage.getItem('projectId') || 'demo-project');
  const [apiKey, setApiKey] = useState(localStorage.getItem('apiKey') || '');

  const [tree, setTree] = useState<GithubTreeItem[]>([]);
  const [stats, setStats] = useState<FileStat[]>([]);
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [chartData, setChartData] = useState<ChartDataPoint[]>([]);
  const [isConnected, setIsConnected] = useState(false);

  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    localStorage.setItem('ghToken', token);
    localStorage.setItem('targetRepo', repo);
    localStorage.setItem('projectId', projectId);
    localStorage.setItem('apiKey', apiKey);
  }, [token, repo, projectId, apiKey]);

  const connectWebSocket = () => {
    if (wsRef.current) wsRef.current.close();

    // Hardcoding to ingest API on localhost:3000 as per docker compose
    const ws = new WebSocket(`ws://localhost:3000/live?apiKey=${encodeURIComponent(apiKey)}`);

    ws.onopen = () => setIsConnected(true);
    ws.onclose = () => setIsConnected(false);

    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === 'rate') {
          setChartData(prev => {
            const timeStr = new Date(payload.timestamp).toLocaleTimeString();
            const next = [...prev, { time: timeStr, calls: payload.calls }];
            if (next.length > 30) next.shift(); // Keep last 30 points
            return next;
          });
        }
      } catch (e) {
        // ignore
      }
    };

    wsRef.current = ws;
  };

  const loadData = async () => {
    let treeDataItems: GithubTreeItem[] = [];

    // 1. Fetch GitHub Tree
    const [owner, repoName] = repo.split('/');
    if (owner && repoName) {
      try {
        const headers: Record<string, string> = {
          'Accept': 'application/vnd.github.v3+json'
        };
        if (token) headers['Authorization'] = `token ${token}`;

        const branchRes = await fetch(`https://api.github.com/repos/${owner}/${repoName}`, { headers });
        if (branchRes.ok) {
          const repoData = await branchRes.json();
          const branch = repoData.default_branch;
          const treeRes = await fetch(`https://api.github.com/repos/${owner}/${repoName}/git/trees/${branch}?recursive=1`, { headers });
          if (treeRes.ok) {
            const treeData = await treeRes.json();
            treeDataItems = treeData.tree.filter((t: any) => t.type === 'blob');
          }
        }
      } catch (e) {
        console.error('Failed fetching github tree', e);
      }
    }

    // 2. Fetch Ingest Stats
    try {
      const statsHeaders: any = {};
      if (apiKey) statsHeaders['x-api-key'] = apiKey;
      const res = await fetch(`http://localhost:3000/stats?projectId=${projectId}&repo=${repo}&hours=24`, {
        headers: statsHeaders
      });
      if (res.ok) {
        const data = await res.json();
        setStats(data.data);

        // Merge stat file paths into the tree if not already there
        const existingPaths = new Set(treeDataItems.map(t => t.path));
        data.data.forEach((stat: FileStat) => {
          if (!existingPaths.has(stat.file_path)) {
            treeDataItems.push({ path: stat.file_path, type: 'blob' });
            existingPaths.add(stat.file_path);
          }
        });
      }
    } catch (e) {
      console.error('Failed fetching stats', e);
    }

    setTree(treeDataItems);
  };

  const handleConnect = () => {
    loadData();
    connectWebSocket();
  };

  // Helper to determine heat of a file
  const getFileHeat = (filePath: string) => {
    const fileStats = stats.filter(s => s.file_path === filePath || filePath.endsWith(s.file_path));
    if (fileStats.length === 0) return 'empty';

    const avgSum = fileStats.reduce((sum, s) => sum + s.avg_duration, 0);
    const avg = avgSum / fileStats.length;
    const totalCalls = fileStats.reduce((sum, s) => sum + Number(s.total_calls), 0);

    if (totalCalls === 0) return 'blue'; // Dead code
    if (avg > 100) return 'red';
    if (avg > 20) return 'orange';
    return 'green';
  };

  const selectedFileStats = useMemo(() => {
    if (!selectedFile) return [];
    return stats.filter(s => s.file_path === selectedFile || selectedFile.endsWith(s.file_path));
  }, [selectedFile, stats]);

  return (
    <div className="dashboard-layout">
      {/* Sidebar Settings */}
      <div className="sidebar">
        <div className="header">
          <h1>CodePulse</h1>
          <p>Runtime Heatmap & Telemetry</p>
        </div>

        <div className="settings-form">
          <div className="input-group">
            <label>GitHub Personal Access Token</label>
            <input
              type="password"
              placeholder="ghp_..."
              value={token}
              onChange={e => setToken(e.target.value)}
            />
          </div>
          <div className="input-group">
            <label>Server API Key</label>
            <input
              type="password"
              placeholder="secret key"
              value={apiKey}
              onChange={e => setApiKey(e.target.value)}
            />
          </div>
          <div className="input-group">
            <label>Target Repository</label>
            <input
              type="text"
              placeholder="owner/repo"
              value={repo}
              onChange={e => setRepo(e.target.value)}
            />
          </div>
          <div className="input-group">
            <label>Project ID</label>
            <input
              type="text"
              placeholder="demo-project"
              value={projectId}
              onChange={e => setProjectId(e.target.value)}
            />
          </div>
          <button className="btn-primary" onClick={handleConnect}>
            {isConnected ? 'Sync Data' : 'Connect & Fetch'}
          </button>
        </div>

        {/* Status indicator */}
        <div className="status-indicator">
          <div className={`heat-indicator ${isConnected ? 'green' : 'red'}`}></div>
          {isConnected ? 'WebSocket Sync On' : 'Connection Lost'}
        </div>
      </div>

      {/* Main Content Area */}
      <div className="main-content">

        <div className="top-panels">
          {/* GitHub File Tree */}
          <div className="panel">
            <div className="panel-header">
              <FolderTree size={20} className="text-accent" color="var(--accent)" />
              File Navigator
            </div>
            <div className="file-tree">
              {tree.length === 0 ? (
                <div style={{ color: 'var(--text-muted)', padding: '1rem' }}>No files detected. Sync data.</div>
              ) : (
                tree.map(node => {
                  const heat = getFileHeat(node.path);
                  return (
                    <div
                      key={node.path}
                      className={`file-item ${selectedFile === node.path ? 'selected' : ''}`}
                      onClick={() => setSelectedFile(node.path)}
                    >
                      <div className={`heat-indicator ${heat}`} />
                      {node.path}
                    </div>
                  )
                })
              )}
            </div>
          </div>

          {/* Functions Table */}
          <div className="panel">
            <div className="panel-header">
              <Code2 size={20} className="text-accent" color="var(--accent)" />
              Function Telemetry {selectedFile && <span style={{ color: 'var(--text-muted)', fontWeight: 400 }}>— {selectedFile.split('/').pop()}</span>}
            </div>
            <div className="functions-table-container">
              {!selectedFile ? (
                <div style={{ color: 'var(--text-muted)', padding: '1rem', textAlign: 'center', marginTop: '2rem' }}>
                  Select a file from the navigator to inspect function telemetry.
                </div>
              ) : selectedFileStats.length === 0 ? (
                <div style={{ color: 'var(--text-muted)', padding: '1rem', textAlign: 'center', marginTop: '2rem' }}>
                  No telemetry captured for this file yet.
                </div>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>Function Signature</th>
                      <th>Invocations</th>
                      <th>Avg Latency</th>
                      <th>p95 Max</th>
                      <th>Source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedFileStats.map((stat, idx) => {
                      const avg = stat.avg_duration;
                      const badgeClass = Number(stat.total_calls) === 0 ? 'dead' : (avg > 100 ? 'slow' : (avg > 20 ? 'medium' : 'fast'));

                      return (
                        <tr key={idx}>
                          <td className="td-func">{stat.function_name}()</td>
                          <td style={{ fontWeight: 600 }}>{stat.total_calls}</td>
                          <td>
                            <span className={`metric-badge ${badgeClass}`}>
                              {Number(stat.avg_duration).toFixed(1)}ms
                            </span>
                          </td>
                          <td style={{ color: 'var(--text-muted)' }}>{Number(stat.p95_duration).toFixed(1)}ms</td>
                          <td>
                            <a
                              href={`https://github.com/${repo}/blame/main/${selectedFile}`}
                              target="_blank"
                              rel="noreferrer"
                              className="github-link"
                            >
                              Trace <ExternalLink size={14} />
                            </a>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>

        {/* Live Recharts Panel */}
        <div className="panel chart-panel">
          <div className="panel-header">
            <Activity size={20} color="var(--accent)" />
            Live Network Throughput
          </div>
          <div className="chart-wrapper">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="colorCalls" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="var(--accent)" stopOpacity={0.6} />
                    <stop offset="95%" stopColor="var(--accent)" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="4 4" stroke="rgba(255,255,255,0.05)" vertical={false} />
                <XAxis dataKey="time" stroke="var(--text-muted)" fontSize={11} tickLine={false} axisLine={false} dy={10} />
                <YAxis stroke="var(--text-muted)" fontSize={11} tickLine={false} axisLine={false} dx={-10} />
                <Tooltip
                  contentStyle={{ backgroundColor: 'rgba(20,20,27,0.9)', borderColor: 'var(--panel-border)', borderRadius: '12px', backdropFilter: 'blur(8px)', boxShadow: '0 8px 32px rgba(0,0,0,0.5)' }}
                  itemStyle={{ color: 'var(--text-primary)', fontWeight: 600 }}
                  labelStyle={{ color: 'var(--text-muted)', fontSize: '0.85rem', marginBottom: '0.2rem' }}
                />
                <Area
                  type="monotone"
                  dataKey="calls"
                  stroke="var(--accent)"
                  strokeWidth={3}
                  fillOpacity={1}
                  fill="url(#colorCalls)"
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

      </div>
    </div>
  );
}

export default App;
