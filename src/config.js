const PORT = parseInt(process.env.PORT || '8080', 10);
const DATA_DIR = process.env.DATA_DIR || '/data/maint';
const MACHINE_ID = process.env.FLY_MACHINE_ID || 'local';

module.exports = {
  port: PORT,
  dataDir: DATA_DIR,
  machineId: MACHINE_ID,
  snapshotEveryEvents: parseInt(process.env.SNAPSHOT_EVERY || '500', 10),
  snapshotIntervalMs: parseInt(process.env.SNAPSHOT_INTERVAL_MS || '60000', 10),
  fsyncOnAppend: process.env.FSYNC_ON_APPEND !== 'false',
  metaFile: 'meta.json',
  eventsFile: 'events.jsonl',
  snapshotFile: 'snapshot.json'
};
