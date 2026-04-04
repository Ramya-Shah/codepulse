exports.getCartItems = (userId) => {
  return [
    { id: 'item1', price: Math.random() * 60 },
    { id: 'item2', price: Math.random() * 40 }
  ];
};

exports.calculateTotal = (items) => {
  return items.reduce((acc, curr) => acc + curr.price, 0);
};

exports.applyDiscountCode = (code) => {
  console.log('Discount routine -> Dead Code');
};
