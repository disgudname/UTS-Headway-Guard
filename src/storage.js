const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const readline = require('readline');
const config = require('./config');
const { DomainState, ticketMatchesPurge } = require('./state');
const logger = require('./logger');

function nowIso() {
  return new Date().toISOString();
}

async function ensureDir(dirPath) {
  await fsp.mkdir(dirPath, { recursive: true });
}

async function readJson(filePath, fallback) {
  try {
    const buf = await fsp.readFile(filePath, 'utf8');
    return JSON.parse(buf);
  } catch (err) {
    if (err.code === 'ENOENT') return fallback;
    throw err;
  }
}

async function writeJsonAtomic(filePath, data) {
  const dir = path.dirname(filePath);
  const tmp = path.join(dir, `${path.basename(filePath)}.${process.pid}.${Date.now()}.tmp`);
  await fsp.writeFile(tmp, JSON.stringify(data, null, 2));
  await fsp.rename(tmp, filePath);
}

class Storage {
  constructor(cfg = config) {
    this.config = cfg;
    this.dataDir = cfg.dataDir;
    this.eventsPath = path.join(this.dataDir, cfg.eventsFile);
    this.snapshotPath = path.join(this.dataDir, cfg.snapshotFile);
    this.metaPath = path.join(this.dataDir, cfg.metaFile);
    this.queueDir = path.join(this.dataDir, cfg.queueDir);
    this.state = new DomainState();
    this.currentOffset = 0;
    this.lastSnapshotOffset = 0;
    this.writeChain = Promise.resolve();
    this.eventsSinceSnapshot = 0;
    this.lastSnapshotAt = Date.now();
    this.eventHandle = null;
    this.pendingAcks = new Map();
    this.localEvents = new Map();
    this.meta = { pending: {}, known_machines: [], last_event_offset: 0 };
    this.snapshotTimer = null;
    this.ackThreshold = cfg.peers && cfg.peers.length > 0 ? cfg.twoAckThreshold : 1;
  }

  transactionPath(txId) {
    return path.join(this.queueDir, `${txId}.json`);
  }

  normaliseAction(action) {
    if (!action || typeof action !== 'object') {
      throw new Error('invalid action');
    }
    const type = action.type;
    if (type === 'append_event') {
      if (!action.event || typeof action.event !== 'object') {
        throw new Error('append_event action missing event');
      }
      const event = JSON.parse(JSON.stringify(action.event));
      if (!event.event_id) {
        throw new Error('event missing event_id');
      }
      return { type: 'append_event', event };
    }
    if (type === 'hard_purge') {
      if (!action.spec || typeof action.spec !== 'object') {
        throw new Error('hard_purge action missing spec');
      }
      const spec = JSON.parse(JSON.stringify(action.spec));
      return { type: 'hard_purge', spec };
    }
    throw new Error(`unsupported action type ${type}`);
  }

  async prepareTransaction(txId, actions) {
    if (!txId || typeof txId !== 'string') {
      throw new Error('transaction_id required');
    }
    if (!Array.isArray(actions) || actions.length === 0) {
      throw new Error('actions required');
    }
    const preparedActions = actions.map((action) => this.normaliseAction(action));
    await ensureDir(this.queueDir);
    const txPath = this.transactionPath(txId);
    const existing = await readJson(txPath, null).catch(() => null);
    if (existing && Array.isArray(existing.actions) && existing.actions.length) {
      return;
    }
    await writeJsonAtomic(txPath, { transaction_id: txId, actions: preparedActions });
  }

  async commitPreparedTransaction(txId) {
    if (!txId || typeof txId !== 'string') {
      throw new Error('transaction_id required');
    }
    const txPath = this.transactionPath(txId);
    const payload = await readJson(txPath, null);
    if (!payload || !Array.isArray(payload.actions) || payload.actions.length === 0) {
      return { committed: false };
    }
    await this.enqueue(async () => {
      for (const action of payload.actions) {
        await this.applyAction(action);
      }
    });
    await fsp.unlink(txPath).catch((err) => {
      if (err.code !== 'ENOENT') {
        throw err;
      }
    });
    return { committed: true };
  }

  async rollbackPreparedTransaction(txId) {
    if (!txId || typeof txId !== 'string') {
      throw new Error('transaction_id required');
    }
    const txPath = this.transactionPath(txId);
    await fsp.unlink(txPath).catch((err) => {
      if (err.code !== 'ENOENT') {
        throw err;
      }
    });
  }

  async applyAction(action) {
    if (!action || typeof action !== 'object') {
      return;
    }
    switch (action.type) {
      case 'append_event':
        await this.commitEvent(action.event, { registerAck: false });
        break;
      case 'hard_purge':
        await this.executeHardPurge(action.spec);
        break;
      default:
        throw new Error(`unsupported action type ${action.type}`);
    }
  }

  async init() {
    await ensureDir(this.dataDir);
    await ensureDir(this.queueDir);
    this.meta = await readJson(this.metaPath, { pending: {}, known_machines: [], last_event_offset: 0, last_compacted_offset: 0 });
    const snapshot = await readJson(this.snapshotPath, null);
    if (snapshot && snapshot.state) {
      this.state = new DomainState(snapshot.state);
      this.currentOffset = snapshot.offset || 0;
      this.lastSnapshotOffset = this.currentOffset;
    } else {
      this.state = new DomainState();
    }

    await this.replayEvents();
    await this.restorePendingAcks();

    this.eventHandle = await fsp.open(this.eventsPath, 'a');
    this.snapshotTimer = setInterval(() => {
      this.enqueue(() => this.saveSnapshot()).catch((err) => {
        logger.error('snapshot error', { err: err.stack });
      });
    }, this.config.snapshotIntervalMs);
  }

  async restorePendingAcks() {
    const metaPending = this.meta.pending || {};
    for (const [eventId, entry] of Object.entries(metaPending)) {
      this.pendingAcks.set(eventId, entry);
      if (!entry.event && this.localEvents.has(eventId)) {
        entry.event = this.localEvents.get(eventId);
      }
    }
    for (const [eventId, event] of this.localEvents.entries()) {
      if (!this.pendingAcks.has(eventId)) {
        this.pendingAcks.set(eventId, {
          event,
          acked: new Set([this.config.machineId])
        });
      }
    }
    for (const [eventId, entry] of this.pendingAcks.entries()) {
      entry.acked = new Set(entry.acked || []);
      if (!entry.acked.has(this.config.machineId)) {
        entry.acked.add(this.config.machineId);
      }
      entry.event = entry.event || this.localEvents.get(eventId);
      entry.nextAttemptAt = entry.nextAttemptAt || 0;
      entry.attempts = entry.attempts || 0;
      if (entry.acked.size >= this.ackThreshold) {
        this.pendingAcks.delete(eventId);
      }
    }
    await this.persistMeta();
  }

  enqueue(fn) {
    this.writeChain = this.writeChain.then(() => fn());
    return this.writeChain;
  }

  async replayEvents() {
    const exists = await fsp
      .stat(this.eventsPath)
      .then(() => true)
      .catch((err) => {
        if (err.code === 'ENOENT') return false;
        throw err;
      });
    if (!exists) {
      await fsp.writeFile(this.eventsPath, '');
      this.currentOffset = 0;
      return;
    }

    const stat = await fsp.stat(this.eventsPath);
    const start = this.currentOffset || 0;
    if (start > stat.size) {
      this.currentOffset = stat.size;
      return;
    }
    const stream = fs.createReadStream(this.eventsPath, {
      encoding: 'utf8',
      start
    });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    for await (const line of rl) {
      if (!line.trim()) continue;
      try {
        const event = JSON.parse(line);
        this.state.applyEvent(event);
        if (event.machine_id === this.config.machineId) {
          this.localEvents.set(event.event_id, event);
        }
      } catch (err) {
        logger.error('failed to parse event line', { err: err.stack });
      }
    }
    this.currentOffset = stat.size;
  }

  async commitEvent(event, { registerAck }) {
    if (!this.eventHandle) {
      this.eventHandle = await fsp.open(this.eventsPath, 'a');
    }
    const line = `${JSON.stringify(event)}\n`;
    await this.eventHandle.appendFile(line, 'utf8');
    if (this.config.fsyncOnAppend) {
      await this.eventHandle.datasync();
    }
    this.currentOffset += Buffer.byteLength(line);
    this.eventsSinceSnapshot += 1;
    const applied = this.state.applyEvent(event);
    if (event.machine_id === this.config.machineId) {
      this.localEvents.set(event.event_id, event);
    }
    if (registerAck) {
      this.registerPendingAck(event);
    }
    await this.persistMeta();
    await this.maybeSnapshot();
    return applied;
  }

  async appendEvent(event) {
    return this.enqueue(() => this.commitEvent(event, { registerAck: true }));
  }

  async persistIncomingEvent(event) {
    if (this.state.seenEventIds.has(event.event_id)) {
      return { applied: false, reason: 'duplicate' };
    }
    const applied = await this.enqueue(() => this.commitEvent(event, { registerAck: false }));
    return { applied };
  }

  registerPendingAck(event) {
    const existing = this.pendingAcks.get(event.event_id);
    if (existing) {
      existing.event = event;
      if (!existing.acked) existing.acked = new Set();
      existing.acked.add(this.config.machineId);
      if (existing.acked.size >= this.ackThreshold) {
        this.pendingAcks.delete(event.event_id);
      }
      return;
    }
    this.pendingAcks.set(event.event_id, {
      event,
      acked: new Set([this.config.machineId]),
      nextAttemptAt: 0,
      attempts: 0
    });
    if (this.ackThreshold <= 1) {
      this.pendingAcks.delete(event.event_id);
    }
  }

  ackEvent(eventId, machineId) {
    const entry = this.pendingAcks.get(eventId);
    if (!entry) return;
    entry.acked.add(machineId);
    if (entry.acked.size >= this.ackThreshold) {
      this.pendingAcks.delete(eventId);
    }
  }

  getPendingEventsForReplication() {
    const pending = [];
    const now = Date.now();
    for (const entry of this.pendingAcks.values()) {
      const ackedCount = entry.acked ? entry.acked.size : 0;
      if (ackedCount >= this.ackThreshold) continue;
      if (entry.nextAttemptAt && entry.nextAttemptAt > now) continue;
      pending.push(entry);
    }
    return pending;
  }

  scheduleRetry(entry, attempt) {
    const base = this.config.writeRetryBaseMs;
    const max = this.config.writeRetryMaxMs;
    const delay = Math.min(max, base * Math.pow(2, attempt));
    const jitter = Math.floor(Math.random() * 500);
    entry.nextAttemptAt = Date.now() + delay + jitter;
  }

  async persistMeta() {
    const serialisable = {};
    for (const [eventId, entry] of this.pendingAcks.entries()) {
      serialisable[eventId] = {
        event: entry.event,
        acked: Array.from(entry.acked || []),
        nextAttemptAt: entry.nextAttemptAt || 0,
        attempts: entry.attempts || 0
      };
    }
    this.meta.pending = serialisable;
    this.meta.last_event_offset = this.currentOffset;
    await writeJsonAtomic(this.metaPath, this.meta);
  }

  async saveSnapshot(force = false) {
    if (!force) {
      if (this.eventsSinceSnapshot < this.config.snapshotEveryEvents) {
        const since = Date.now() - this.lastSnapshotAt;
        if (since < this.config.snapshotIntervalMs) {
          return;
        }
      }
    }
    const snapshot = {
      state: this.state.serialize(),
      offset: this.currentOffset,
      created_at: nowIso()
    };
    await writeJsonAtomic(this.snapshotPath, snapshot);
    this.lastSnapshotAt = Date.now();
    this.eventsSinceSnapshot = 0;
    this.lastSnapshotOffset = this.currentOffset;
  }

  async maybeSnapshot() {
    await this.saveSnapshot(false);
  }

  async reloadStateFromScratch() {
    this.state = new DomainState();
    this.currentOffset = 0;
    this.localEvents.clear();
    this.pendingAcks.clear();
    this.meta.pending = {};
    await this.replayEvents();
    await this.restorePendingAcks();
    await this.saveSnapshot(true);
  }

  async performHardPurge(purgeSpec) {
    await this.enqueue(() => this.executeHardPurge(purgeSpec));
  }

  async executeHardPurge(purgeSpec) {
    await this.saveSnapshot(true);
    if (this.eventHandle) {
      await this.eventHandle.close();
      this.eventHandle = null;
    }
    const tmpPath = path.join(this.dataDir, `events-${Date.now()}-${process.pid}.tmp`);
    const readStream = fs.createReadStream(this.eventsPath, { encoding: 'utf8' });
    const rl = readline.createInterface({ input: readStream, crlfDelay: Infinity });
    const writeHandle = await fsp.open(tmpPath, 'w');
    const purgeSet = new Set();
    for (const ticket of this.state.tickets.values()) {
      if (ticketMatchesPurge(ticket, this.state.vehicles, purgeSpec)) {
        purgeSet.add(ticket.id);
      }
    }
    for await (const line of rl) {
      if (!line.trim()) {
        continue;
      }
      const event = JSON.parse(line);
      if (purgeSet.has(event.payload?.ticket?.id)) {
        continue;
      }
      await writeHandle.appendFile(`${JSON.stringify(event)}\n`, 'utf8');
    }
    await writeHandle.datasync();
    await writeHandle.close();
    await fsp.rename(tmpPath, this.eventsPath);
    await this.reloadStateFromScratch();
    this.meta.last_compacted_offset = this.currentOffset;
    await this.persistMeta();
    this.eventHandle = await fsp.open(this.eventsPath, 'a');
  }

  async eventsSince(ts) {
    const results = [];
    const exists = await fsp
      .stat(this.eventsPath)
      .then(() => true)
      .catch((err) => {
        if (err.code === 'ENOENT') return false;
        throw err;
      });
    if (!exists) return results;
    const stream = fs.createReadStream(this.eventsPath, { encoding: 'utf8' });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    for await (const line of rl) {
      if (!line.trim()) continue;
      try {
        const event = JSON.parse(line);
        if (!ts || event.ts >= ts) {
          results.push(event);
        }
      } catch (err) {
        logger.error('failed to parse event during export', { err: err.stack });
      }
    }
    return results;
  }
}

module.exports = {
  Storage
};
