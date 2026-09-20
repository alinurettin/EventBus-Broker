class CoreEngine {
  constructor() {
    this.state = new Map();
    this.history = [];
  }
  process(payload) {
    if (!payload || typeof payload !== 'object') throw new Error('Invalid payload object');
    const id = payload.id || ('job-' + Date.now() + '-' + Math.floor(Math.random() * 1000));
    const processedAt = new Date().toISOString();
    const result = {
      id,
      input: payload,
      processedAt,
      status: 'PROCESSED',
      executionScore: parseFloat((Math.random() * 0.2 + 0.8).toFixed(4))
    };
    this.state.set(id, result);
    this.history.push(id);
    if (this.history.length > 500) {
      const old = this.history.shift();
      this.state.delete(old);
    }
    return result;
  }
  get(id) {
    return this.state.get(id) || null;
  }
  count() {
    return this.state.size;
  }
}
module.exports = CoreEngine;
