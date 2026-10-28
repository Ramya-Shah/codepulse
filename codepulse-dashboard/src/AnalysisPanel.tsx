import { useCallback, useEffect, useState } from 'react';
import { Sparkles } from 'lucide-react';
import { fetchAnalysis, requestAnalysis, type AnalysisResult } from './api';

interface Props {
  baseUrl: string;
  projectId: string;
  repo: string;
  apiKey?: string;
  isDemo: boolean;
}

type State =
  | { status: 'loading' }
  | { status: 'disabled' }
  | { status: 'idle' }
  | { status: 'generating' }
  | { status: 'ready'; result: AnalysisResult }
  | { status: 'error'; message: string };

function timeAgo(iso: string): string {
  const minutes = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  return hours < 48 ? `${hours} h ago` : `${Math.round(hours / 24)} days ago`;
}

// AI root-cause analysis of the current hotspot. The parent keys this by
// hotspot, so a new hotspot starts from a fresh state.
export function AnalysisPanel({ baseUrl, projectId, repo, apiKey, isDemo }: Props) {
  const [state, setState] = useState<State>({ status: 'loading' });

  const generate = useCallback(async () => {
    setState({ status: 'generating' });
    try {
      const result = await requestAnalysis(baseUrl, projectId, repo, apiKey);
      setState({ status: 'ready', result });
    } catch (err) {
      setState({ status: 'error', message: err instanceof Error ? err.message : 'Analysis failed' });
    }
  }, [baseUrl, projectId, repo, apiKey]);

  useEffect(() => {
    let cancelled = false;
    fetchAnalysis(baseUrl, projectId, repo, apiKey)
      .then(data => {
        if (cancelled) return;
        if (!data.configured && !data.result) setState({ status: 'disabled' });
        else if (data.result) setState({ status: 'ready', result: data.result });
        // The demo explains itself; the server caches it, so this is cheap.
        else if (isDemo) generate();
        else setState({ status: 'idle' });
      })
      .catch(() => !cancelled && setState({ status: 'idle' }));
    return () => { cancelled = true; };
  }, [baseUrl, projectId, repo, apiKey, isDemo, generate]);

  return (
    <div className="analysis">
      <div className="analysis-title">
        <Sparkles size={16} /> AI root cause
      </div>

      {state.status === 'loading' && <p className="analysis-note"><span className="loading-dots">Loading analysis</span></p>}

      {state.status === 'disabled' && <p className="analysis-note">AI analysis isn't enabled on this server (no model key configured).</p>}

      {state.status === 'idle' && (
        <button className="btn-primary" onClick={generate}><Sparkles size={15} /> Explain this bottleneck</button>
      )}

      {state.status === 'generating' && (
        <p className="analysis-note"><span className="loading-dots">Reading the telemetry and source code</span></p>
      )}

      {state.status === 'error' && (
        <div className="analysis-note">
          {state.message}{' '}
          <button className="link-button" onClick={generate}>Try again</button>
        </div>
      )}

      {state.status === 'ready' && <AnalysisBody result={state.result} />}
    </div>
  );
}

function AnalysisBody({ result }: { result: AnalysisResult }) {
  const { analysis, evidence } = result;
  const hot = evidence.hotspot;

  return (
    <>
      <p className="analysis-headline">{analysis.headline}</p>
      <p>{analysis.root_cause}</p>

      <h4>Evidence</h4>
      <ul className="analysis-evidence">
        {analysis.evidence.map((line, i) => <li key={i}>{line}</li>)}
      </ul>

      <h4>Fix</h4>
      <p>{analysis.fix}</p>
      <details className="analysis-code">
        <summary>Suggested code</summary>
        <pre>{analysis.fixed_code}</pre>
      </details>
      <p className="analysis-impact"><strong>Expected impact:</strong> {analysis.expected_impact}</p>

      <div className="analysis-grounding">
        <span className={`confidence ${analysis.confidence}`}>{analysis.confidence} confidence</span>
        <span>
          Based on <code>{hot.function_name}()</code>: {hot.calls.toLocaleString()} calls, avg {hot.avg_ms}ms, p95 {hot.p95_ms}ms
          over {evidence.window_minutes} min{evidence.related.length > 0 && <>, plus {evidence.related.length} related functions</>}
          {evidence.source_files.length > 0 && <>, and the source of {evidence.source_files.join(', ')}</>}.
        </span>
        <span>Generated {timeAgo(result.created_at)} by {result.model}.</span>
      </div>
    </>
  );
}
