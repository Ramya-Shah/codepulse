import Module from 'module';
import { performance } from 'perf_hooks';
import http from 'http';
import https from 'https';

export interface InitOptions {
  ingestUrl: string;
  projectId: string;
  githubRepo: string;
  apiKey?: string;
}

interface CallStat {
  call_count: number;
  total_duration_ms: number;
  last_timestamp: string;
}

let options: InitOptions | null = null;
let statsBuffer = new Map<string, CallStat>();
let intervalId: NodeJS.Timeout | null = null;
const wrappedCache = new WeakMap<any, any>();

function formatTimestamp(): string {
  const date = new Date();
  return date.toISOString().replace('T', ' ').substring(0, 19);
}

function recordStats(filePath: string, funcName: string, durationMs: number) {
  if (!options) return;
  
  const relativePath = filePath.split(process.cwd())[1]?.replace(/^[\\\/]/, '') || filePath;
  const key = `${relativePath}::${funcName}`;
  const existing = statsBuffer.get(key) || { call_count: 0, total_duration_ms: 0, last_timestamp: '' };
  
  existing.call_count++;
  existing.total_duration_ms += durationMs;
  existing.last_timestamp = formatTimestamp();
  
  statsBuffer.set(key, existing);
}

function wrapFunction(fn: Function, filePath: string, funcName: string) {
  return new Proxy(fn, {
    apply(target, thisArg, argumentsList) {
      const start = performance.now();
      try {
        const result = Reflect.apply(target, thisArg, argumentsList);
        if (result instanceof Promise) {
           return result.then(val => {
             recordStats(filePath, funcName, performance.now() - start);
             return val;
           }).catch(err => {
             recordStats(filePath, funcName, performance.now() - start);
             throw err;
           });
        }
        recordStats(filePath, funcName, performance.now() - start);
        return result;
      } catch (err) {
        recordStats(filePath, funcName, performance.now() - start);
        throw err;
      }
    }
  });
}

function wrapExports(exportsObj: any, filePath: string, defaultName: string = 'default'): any {
  if (typeof exportsObj === 'function') {
      return wrapFunction(exportsObj, filePath, exportsObj.name || defaultName);
  } else if (typeof exportsObj === 'object' && exportsObj !== null) {
      return new Proxy(exportsObj, {
        get(target, prop, receiver) {
          const val = Reflect.get(target, prop, receiver);
          if (typeof val === 'function') {
            return wrapFunction(val, filePath, String(prop));
          }
          return val;
        }
      });
  }
  return exportsObj;
}

function flushStats() {
  if (!options || statsBuffer.size === 0) return;

  const payload = Array.from(statsBuffer.entries()).map(([key, stat]) => {
    const [file_path, function_name] = key.split('::');
    return {
      project_id: options!.projectId,
      repo: options!.githubRepo,
      file_path,
      function_name,
      call_count: stat.call_count,
      avg_duration_ms: stat.total_duration_ms / stat.call_count,
      timestamp: stat.last_timestamp,
    };
  });
  
  statsBuffer.clear();

  try {
    const url = new URL(options.ingestUrl);
    const client = url.protocol === 'https:' ? https : http;
    
    const headers: any = { 'Content-Type': 'application/json' };
    if (options.apiKey) headers['x-api-key'] = options.apiKey;

    const req = client.request(url, {
      method: 'POST',
      headers
    });

    req.on('error', (err) => {
      console.error('[CodePulse] Error flushing stats:', err.message);
    });
    
    req.write(JSON.stringify(payload));
    req.end();
  } catch (err) {
    console.error('[CodePulse] Failed to parse ingestUrl:', err);
  }
}

export function init(opts: InitOptions) {
  if (options) {
    console.warn('[CodePulse] Already initialized');
    return;
  }
  options = opts;

  const originalLoad = (Module as any)._load;
  (Module as any)._load = function(request: string, parent: any, isMain: boolean) {
    const exports = originalLoad.apply(this, arguments);
    
    try {
      if (options && typeof request === 'string') {
        const filename = (Module as any)._resolveFilename(request, parent, isMain);
        if (filename && !filename.includes('node_modules') && !filename.startsWith('node:') && require('path').isAbsolute(filename)) {
          if ((typeof exports === 'function' || typeof exports === 'object') && exports !== null) {
            if (!wrappedCache.has(exports)) {
              const wrapped = wrapExports(exports, filename);
              wrappedCache.set(exports, wrapped);
              return wrapped;
            } else {
              return wrappedCache.get(exports);
            }
          }
        }
      }
    } catch (e) {
    }
    
    return exports;
  };

  intervalId = setInterval(flushStats, 5000);
  intervalId.unref(); 
  
  console.log('[CodePulse] SDK initialized for project:', opts.projectId);
}
