// In-memory stand-in for a database. Each call waits for a simulated round trip
// so latency behaves like a real query over the network.
const roundTrip = (minMs, maxMs) => new Promise(resolve => setTimeout(resolve, minMs + Math.random() * (maxMs - minMs)));

exports.queryUsers = async () => {
  await roundTrip(2, 8);
  return [{ id: 'uid_123', name: 'Alice' }];
};

exports.getOrdersForUser = async (userId) => {
  await roundTrip(3, 8);
  // 4 orders with 3-6 line items each
  return Array.from({ length: 4 }, (_, i) => ({
    id: `ord_${userId}_${i}`,
    productIds: Array.from({ length: 3 + ((i * 7) % 4) }, (_, j) => `prod_${i}_${j}`)
  }));
};

exports.getProductById = async (productId) => {
  await roundTrip(4, 8);
  return { id: productId, name: `Product ${productId}`, price: 499 };
};

exports.getProductsByIds = async (productIds) => {
  await roundTrip(4, 9);
  return productIds.map(id => ({ id, name: `Product ${id}`, price: 499 }));
};

exports.vacuumDatabaseTables = () => {
  // CRON job that never triggers in this environment (Dead Code)
};
