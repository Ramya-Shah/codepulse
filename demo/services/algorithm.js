const crypto = require('crypto');

exports.heavyCrypto = () => {
  // Heavy CPU blocking logic to simulate poor algorithm performance
  let hash = '';
  for(let i=0; i<150; i++) {
    hash = crypto.createHash('sha256').update(Math.random().toString()).digest('hex');
  }
  return hash;
}

exports.quickHash = () => {
  // Fast hashing algorithm
  return crypto.createHash('md5').update('fast').digest('hex');
}

exports.deprecatedHashAlgorithm = () => {
  // Completely dead legacy code
  console.log('OOM Error Risk - Deprecated');
}
