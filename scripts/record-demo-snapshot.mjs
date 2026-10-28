// Records real data from a running CodePulse backend into the dashboard's
// replay file, which is shown when the live backend is unreachable.
//
//   node scripts/record-demo-snapshot.mjs [apiUrl] [seconds]
//
// Defaults: http://localhost:3000, 60 seconds of live call rate.
import { writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const apiUrl = (process.argv[2] || 'http://localhost:3000').replace(/\/+$/, '');
const seconds = Number(process.argv[3] || 60);
const out = fileURLToPath(new URL('../codepulse-dashboard/public/demo-snapshot.json', import.meta.url));

const getJson = async (path) => {
  const res = await fetch(`${apiUrl}${path}`);
  if (!res.ok) throw new Error(`${path}: ${res.status}`);
  return res.json();
};

const demo = await getJson('/demo');
const q = `projectId=${encodeURIComponent(demo.projectId)}&repo=${encodeURIComponent(demo.repo)}`;

console.log(`Recording ${seconds}s of live call rate from ${apiUrl}...`);
const rates = await new Promise((resolve, reject) => {
  const values = [];
  const ws = new WebSocket(`${apiUrl.replace(/^http/, 'ws')}/live?projectId=${encodeURIComponent(demo.projectId)}`);
  ws.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    if (payload.type === 'rate') values.push(payload.calls);
    if (values.length >= seconds) { ws.close(); resolve(values); }
  };
  ws.onerror = () => reject(new Error('WebSocket failed'));
});

const [{ files }, { data: stats, window_minutes }, analysis] = await Promise.all([
  getJson(`/tree?projectId=${encodeURIComponent(demo.projectId)}`),
  getJson(`/stats?${q}&minutes=15`),
  getJson(`/analysis?${q}`)
]);

if (!stats.length) throw new Error('No stats recorded; is the demo app sending data?');
if (!analysis.result) console.warn('Warning: no cached AI analysis; the replay will have none.');

const snapshot = {
  recorded_at: new Date().toISOString(),
  window_minutes,
  projectId: demo.projectId,
  repo: demo.repo,
  files,
  stats,
  rates,
  analysis: analysis.result
};

await writeFile(out, JSON.stringify(snapshot, null, 2) + '\n');
console.log(`Wrote ${out}: ${stats.length} functions, ${rates.length}s of rates, analysis: ${analysis.result ? 'yes' : 'no'}`);
