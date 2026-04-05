const { init } = require('codepulse-sdk');
init({
  ingestUrl: process.env.INGEST_URL || 'http://localhost:3000/ingest',
  projectId: 'demo-project',
  githubRepo: 'Ramya-Shah/codepulse',
  apiKey: process.env.API_KEY || 'codepulse-secret-key-12345'
});

const algo = require('./services/algorithm');
const db = require('./services/database');
const auth = require('./services/auth');
const cart = require('./services/cart');
const payment = require('./services/payment');

// Emulate an extremely complex, branching real-world application request cycle
async function simulateRequest() {
  try {
    // Phase 1: Authentication and Basic Data
    await auth.verifyToken('mock-user-session');
    
    // Phase 2: Route branching based on RNG pseudo-behavior
    const randomSeed = Math.random();
    
    if (randomSeed > 0.8) {
      // 20% scenario: Massive Admin Analytic Dashboard Load (SLOW RED TAGS)
      algo.heavyCrypto();
      await db.deepJoinAnalytics();
    } else {
      // 80% scenario: Normal quick browsing (GREEN TAGS)
      algo.quickHash();
      await db.queryUsers();
      
      const items = cart.getCartItems('uid_123');
      const total = cart.calculateTotal(items);
      
      // Phase 3: Checkout logic
      if (total > 80) {
        // High purchase amount -> Secure Stripe gateway
        await payment.processCreditCard(total);
      } else {
        // Low amount -> Speedy Wallet checkout
        await payment.processWallet(total);
      }
    }
  } catch (e) {
    // ignore
  }
}

console.log('▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬');
console.log('🚀 BOOTING HYPER-SCALE TRAFFIC SIMULATOR 🚀');
console.log('▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬');
console.log('Sending ~5,000 concurrent functional jumps per second into the CodePulse SDK...');
console.log('Open your dashboard to visualize the chaos.');

// Spawn 10 simultaneous pulse clusters looping at 50ms intervals
// Each pulse handles 25 parallel requests.
// 10 * (1000/50)=10*20=200 pulses/sec * 25 requests = 5,000 invocations simulated per second!
for (let processFork = 0; processFork < 10; processFork++) {
  setInterval(() => {
    for (let req = 0; req < 25; req++) {
      simulateRequest();
    }
  }, 50);
}
