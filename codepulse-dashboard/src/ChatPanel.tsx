import { useEffect, useRef, useState, type FormEvent, type ReactNode } from 'react';
import { Send } from 'lucide-react';
import { DEFAULT_SUGGESTIONS, fetchSuggestions, sendChat, type ChatMessage } from './api';

interface Props {
  baseUrl: string;
  projectId: string;
  repo: string;
  apiKey?: string;
  // Recorded answers, used instead of the server in replay mode.
  replayAnswers?: { question: string; answer: string }[];
}

const MAX_CHARS = 600;
const MAX_HISTORY = 8;

// Renders the model's light markdown (code spans, fenced code, bullets) as
// elements. Never injects HTML.
function renderAnswer(text: string): ReactNode[] {
  const blocks = text.split(/```(?:\w+)?\n?/);
  return blocks.flatMap((block, i) => {
    if (i % 2 === 1) return [<pre key={i}>{block.replace(/\n$/, '')}</pre>];
    return block.split(/\n{2,}/).filter(p => p.trim()).map((para, j) => {
      const lines = para.split('\n');
      if (lines.every(l => /^\s*[-*]\s+/.test(l))) {
        return <ul key={`${i}-${j}`}>{lines.map((l, k) => <li key={k}>{inline(l.replace(/^\s*[-*]\s+/, ''))}</li>)}</ul>;
      }
      return <p key={`${i}-${j}`}>{lines.map((l, k) => <span key={k}>{k > 0 && <br />}{inline(l)}</span>)}</p>;
    });
  });
}

function inline(text: string): ReactNode[] {
  return text.split(/(`[^`]+`|\*\*[^*]+\*\*|<code>[^<]+<\/code>)/).map((part, i) => {
    if (part.startsWith('`') && part.endsWith('`')) return <code key={i}>{part.slice(1, -1)}</code>;
    if (part.startsWith('<code>')) return <code key={i}>{part.slice(6, -7)}</code>;
    if (part.startsWith('**') && part.endsWith('**')) return <strong key={i}>{part.slice(2, -2)}</strong>;
    return part;
  });
}

export function ChatPanel({ baseUrl, projectId, repo, apiKey, replayAnswers }: Props) {
  const isReplay = replayAnswers !== undefined;
  const [suggestions, setSuggestions] = useState<string[]>(() =>
    isReplay ? replayAnswers.map(a => a.question) : DEFAULT_SUGGESTIONS
  );
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [pending, setPending] = useState(false);
  const [slow, setSlow] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (isReplay) return;
    let cancelled = false;
    fetchSuggestions(baseUrl).then(q => !cancelled && q.length && setSuggestions(q)).catch(() => {});
    return () => { cancelled = true; };
  }, [baseUrl, isReplay]);

  // After 10s, say the model is busy rather than look frozen.
  useEffect(() => {
    if (!pending) return;
    const id = window.setTimeout(() => setSlow(true), 10000);
    return () => window.clearTimeout(id);
  }, [pending]);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
  }, [messages, pending]);

  const ask = async (question: string) => {
    const text = question.trim().slice(0, MAX_CHARS);
    if (!text || pending) return;
    const history: ChatMessage[] = [...messages, { role: 'user' as const, text }].slice(-MAX_HISTORY);
    setMessages(history);
    setInput('');
    setError(null);

    if (isReplay) {
      const recorded = replayAnswers.find(a => a.question === text);
      setMessages([...history, { role: 'model', text: recorded ? recorded.answer : 'The live backend is offline, so only the suggested questions have recorded answers right now.' }]);
      return;
    }

    setSlow(false);
    setPending(true);
    try {
      const answer = await sendChat(baseUrl, projectId, repo, history, apiKey);
      setMessages([...history, { role: 'model', text: answer }]);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Something went wrong.');
      setMessages(messages);
      setInput(text);
    } finally {
      setPending(false);
    }
  };

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    ask(input);
  };

  const asked = new Set(messages.filter(m => m.role === 'user').map(m => m.text));
  const remaining = suggestions.filter(q => !asked.has(q));

  return (
    <div className="chat">
      <div className="chat-scroll" ref={scrollRef}>
        {messages.length === 0 && (
          <p className="chat-intro">
            Ask about any function in the demo app. Answers use the live numbers on this page and the app's source code.
          </p>
        )}
        {messages.map((m, i) => (
          <div key={i} className={`chat-msg ${m.role}`}>
            {m.role === 'model' ? renderAnswer(m.text) : m.text}
          </div>
        ))}
        {pending && (
          <div className="chat-msg model">
            <span className="loading-dots">{slow ? 'The AI model is busy right now, still working' : 'Reading the telemetry and code'}</span>
          </div>
        )}
        {error && <div className="chat-error">{error}</div>}
        {remaining.length > 0 && !pending && (
          <div className="chat-suggestions">
            {remaining.map(q => (
              <button key={q} type="button" onClick={() => ask(q)}>{q}</button>
            ))}
          </div>
        )}
      </div>
      <form className="chat-input" onSubmit={onSubmit}>
        <input
          value={input}
          onChange={e => setInput(e.target.value)}
          placeholder={isReplay ? 'Pick a suggested question (backend offline)' : 'Ask why a function is slow…'}
          maxLength={MAX_CHARS}
          disabled={pending || isReplay}
          aria-label="Ask about the code"
        />
        <button type="submit" disabled={pending || isReplay || !input.trim()} aria-label="Send"><Send size={16} /></button>
      </form>
    </div>
  );
}
