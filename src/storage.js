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
    this.state = new DomainState();
    this.currentOffset = 0;
    this.lastSnapshotOffset = 0;
    this.writeChain = Promise.resolve();
    this.eventsSinceSnapshot = 0;
    this.lastSnapshotAt = Date.now();
    this.eventHandle = null;
    this.meta = { last_event_offset: 0, last_compacted_offset: 0 };
    this.snapshotTimer = null;
  }

  async init() {
    await ensureDir(this.dataDir);
    this.meta = await readJson(this.metaPath, { last_event_offset: 0, last_compacted_offset: 0 });
    const snapshot = await readJson(this.snapshotPath, null);
    if (snapshot && snapshot.state) {
      this.state = new DomainState(snapshot.state);
      this.currentOffset = snapshot.offset || 0;
      this.lastSnapshotOffset = this.currentOffset;
    } else {
      this.state = new DomainState();
    }

    await this.replayEvents();
    this.meta.last_event_offset = this.currentOffset;
    await this.persistMeta();

    this.eventHandle = await fsp.open(this.eventsPath, 'a');
    this.snapshotTimer = setInterval(() => {
      this.enqueue(() => this.saveSnapshot()).catch((err) => {
        logger.error('snapshot error', { err: err.stack });
      });
    }, this.config.snapshotIntervalMs);
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
      } catch (err) {
        logger.error('failed to parse event line', { err: err.stack });
      }
    }
    this.currentOffset = stat.size;
  }

  async commitEvent(event) {
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
    await this.persistMeta();
    await this.saveSnapshot(false);
    return applied;
  }

  async appendEvent(event) {
    return this.enqueue(() => this.commitEvent(event));
  }

  async persistMeta() {
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

  async reloadStateFromScratch() {
    this.state = new DomainState();
    this.currentOffset = 0;
    await this.replayEvents();
    this.meta.last_event_offset = this.currentOffset;
    await this.persistMeta();
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
    this.eventHandle = await fsp.open(this.eventsPath, 'a');
    this.meta.last_compacted_offset = this.currentOffset;
    await this.persistMeta();
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
