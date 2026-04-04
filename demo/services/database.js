exports.queryUsers = () => {
  return new Promise(resolve => setTimeout(resolve, Math.random() * 10)); // Super fast indexed queries
}

exports.deepJoinAnalytics = () => {
  return new Promise(resolve => {
    // Massive simulated latency for unindexed DB table joins (forces Red Heatmap)
    setTimeout(resolve, 150 + Math.random() * 150); 
  }); 
}

exports.vacuumDatabaseTables = () => {
  // CRON job that never triggers in this environment (Dead Code)
}
