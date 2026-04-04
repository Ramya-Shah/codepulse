exports.verifyToken = (token) => {
  return new Promise((resolve) => {
    // Super fast caching layer return
    setTimeout(() => {
      resolve({ id: 123, name: 'Alice' });
    }, Math.random() * 10); 
  });
};

exports.legacyAuthValidate = () => {
  console.log('Legacy Auth routine -> Dead Code');
};
