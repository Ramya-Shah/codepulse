exports.processCreditCard = (amount) => {
  return new Promise((resolve) => {
    // Heavy External API dependency simulating SLOW execution
    setTimeout(() => {
      resolve({ status: 'paid', gateway: 'stripe' });
    }, 150 + Math.random() * 80); // Red > 100ms
  });
};

exports.processWallet = (amount) => {
  return new Promise((resolve) => {
    // Mild latency simulating local processing
    setTimeout(() => {
      resolve({ status: 'paid', gateway: 'wallet' });
    }, 40 + Math.random() * 30); // Orange ~60ms
  });
};
