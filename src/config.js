const path = require('path');

const PORT = parseInt(process.env.PORT || '8080', 10);
const DATA_DIR = process.env.DATA_DIR || '/data/maint';
const MACHINE_ID = process.env.FLY_MACHINE_ID || 'local';

function parsePeers() {
  if (!process.env.PEERS) return [];
  return process.env.PEERS.split(',')
    .map((p) => p.trim())
    .filter(Boolean);
}

module.exports = {
  port: PORT,
  dataDir: DATA_DIR,
  machineId: MACHINE_ID,
  peers: parsePeers(),
  snapshotEveryEvents: parseInt(process.env.SNAPSHOT_EVERY || '500', 10),
  snapshotIntervalMs: parseInt(process.env.SNAPSHOT_INTERVAL_MS || '60000', 10),
  fsyncOnAppend: process.env.FSYNC_ON_APPEND !== 'false',
  twoAckThreshold: 2,
  writeRetryBaseMs: parseInt(process.env.REPL_RETRY_BASE_MS || '2000', 10),
  writeRetryMaxMs: parseInt(process.env.REPL_RETRY_MAX_MS || '30000', 10),
  metaFile: 'meta.json',
  eventsFile: 'events.jsonl',
  snapshotFile: 'snapshot.json',
  queueDir: 'queue'
};
