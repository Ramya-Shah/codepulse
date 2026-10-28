const db = require('./database');

// Loads a user's recent orders with product details for the "My orders" page.
exports.getOrderHistory = async (userId) => {
  const orders = await db.getOrdersForUser(userId);
  const result = [];
  for (const order of orders) {
    const items = [];
    for (const productId of order.productIds) {
      items.push(await db.getProductById(productId));
    }
    result.push({ ...order, items });
  }
  return result;
};

exports.getOrderSummary = async (userId) => {
  const orders = await db.getOrdersForUser(userId);
  return { userId, orderCount: orders.length };
};
