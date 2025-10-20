function now() {
  return new Date().toISOString();
}

function log(level, message, meta) {
  const base = { ts: now(), level, msg: message };
  const payload = meta ? { ...base, ...meta } : base;
  console.log(JSON.stringify(payload));
}

module.exports = {
  info: (msg, meta) => log('info', msg, meta),
  error: (msg, meta) => log('error', msg, meta)
};
