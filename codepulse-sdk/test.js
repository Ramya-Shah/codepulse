const { init } = require('./dist/index.js');
const http = require('http');

// Dummy ingest server to capture the SDK payload
const server = http.createServer((req, res) => {
  if (req.method === 'POST' && req.url === '/ingest') {
    let body = '';
    req.on('data', chunk => body += chunk.toString());
    req.on('end', () => {
      console.log('\n--- Received Payload from SDK ---');
      console.log(JSON.stringify(JSON.parse(body), null, 2));
      console.log('---------------------------------');
      res.end('ok');
      process.exit(0);
    });
  }
});
server.listen(3005, () => {
  console.log('Dummy ingest server on :3005');
  
  // Initialize SDK
  init({
    ingestUrl: 'http://localhost:3005/ingest',
    projectId: 'test-project',
    githubRepo: 'test-repo'
  });

  // Require local module *after* init to trigger the hook
  const math = require('./test-math.js');
  
  console.log('Calling math functions...');
  math.add(5, 10);
  math.sub(10, 5);
  
  // Wait for SDK to flush (5s)
  console.log('Waiting 6s for SDK to flush...');
});
