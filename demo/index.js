const path = require('path');
const { init } = require('codepulse-sdk');

init({
  ingestUrl: process.env.INGEST_URL || 'http://localhost:3000/ingest',
  projectId: process.env.PROJECT_ID || 'demo-project',
  githubRepo: process.env.GITHUB_REPO || 'Ramya-Shah/CodePulse',
  apiKey: process.env.API_KEY,
  // Report paths relative to the repo root (demo/services/...) to match GitHub.
  rootDir: path.resolve(__dirname, '..')
});

const algo = require('./services/algorithm');
const db = require('./services/database');
const auth = require('./services/auth');
const cart = require('./services/cart');
const orders = require('./services/orders');
const payment = require('./services/payment');

// One simulated request to a small online store.
async function simulateRequest() {
  try {
    await auth.verifyToken('mock-user-session');
    const route = Math.random();

    if (route < 0.55) {
      // Browse: fast
      algo.quickHash();
      await db.queryUsers();
      cart.calculateTotal(cart.getCartItems('uid_123'));
    } else if (route < 0.8) {
      // "My orders" page: the N+1 bottleneck
      await orders.getOrderHistory('uid_123');
    } else if (route < 0.9) {
      await orders.getOrderSummary('uid_123');
    } else {
      // Checkout
      const total = cart.calculateTotal(cart.getCartItems('uid_123'));
      if (total > 80) await payment.processCreditCard(total);
      else await payment.processWallet(total);
    }
  } catch (e) {
    // ignore
  }
}

const RPS = Number(process.env.REQUESTS_PER_SECOND || 40);
console.log(`[demo] Simulating ~${RPS} requests/sec against the CodePulse SDK`);

setInterval(() => {
  for (let i = 0; i < RPS / 10; i++) simulateRequest();
}, 100);
