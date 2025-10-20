const { URL } = require('url');

const config = require('./config');
const logger = require('./logger');

class Replicator {
  constructor(storage, cfg = config) {
    this.storage = storage;
    this.config = cfg;
    this.peers = cfg.peers;
    this.machineId = cfg.machineId;
    this.running = false;
    this.timer = null;
  }

  async start() {
    if (!this.peers.length) {
      return;
    }
    this.running = true;
    await this.startupSync();
    this.schedule(0);
  }

  stop() {
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
  }

  schedule(delay) {
    if (!this.running) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.process().catch((err) => {
        logger.error('replication loop failure', { err: err.stack });
        this.schedule(this.config.writeRetryBaseMs);
      });
    }, delay);
  }

  async process() {
    if (!this.running) return;
    const pending = this.storage.getPendingEventsForReplication();
    if (!pending.length) {
      this.schedule(this.config.writeRetryBaseMs);
      return;
    }
    for (const entry of pending) {
      await this.replicateEntry(entry);
    }
    this.schedule(this.config.writeRetryBaseMs);
  }

  async replicateEntry(entry) {
    if (!entry || !entry.event) return;
    const event = entry.event;
    const payload = JSON.stringify({ events: [event], from_machine: this.machineId });
    for (const peer of this.peers) {
      try {
        const url = new URL('/internal/replicate', peer).toString();
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: payload,
          signal: AbortSignal.timeout(10000)
        });
        if (!res.ok) {
          throw new Error(`Peer ${peer} responded ${res.status}`);
        }
        const data = await res.json();
        if (data?.ack) {
          const machineId = data.machine_id;
          if (machineId) {
            this.storage.ackEvent(event.event_id, machineId);
            await this.storage.persistMeta();
          }
        }
        logger.info('replicated event', { event_id: event.event_id, peer });
      } catch (err) {
        entry.attempts = (entry.attempts || 0) + 1;
        this.storage.scheduleRetry(entry, entry.attempts);
        await this.storage.persistMeta();
        logger.error('replication failed', { peer, err: err.message });
      }
    }
  }

  async startupSync() {
    const sinceTs = this.storage.state.lastEventTs;
    for (const peer of this.peers) {
      try {
        const url = new URL('/internal/export', peer);
        if (sinceTs) {
          url.searchParams.set('since_ts', sinceTs);
        }
        const res = await fetch(url.toString(), {
          method: 'GET',
          headers: { accept: 'application/json' },
          signal: AbortSignal.timeout(10000)
        });
        if (!res.ok) {
          throw new Error(`sync failed: ${res.status}`);
        }
        const events = await res.json();
        if (Array.isArray(events)) {
          for (const event of events) {
            const result = await this.storage.persistIncomingEvent(event);
            if (result.applied && event.type === 'purge.requested' && event.payload?.mode === 'hard') {
              await this.storage.performHardPurge(event.payload);
            }
          }
        }
        logger.info('startup sync from peer complete', { peer, count: events?.length || 0 });
      } catch (err) {
        logger.error('startup sync error', { peer, err: err.message });
      }
    }
  }
}

module.exports = {
  Replicator
};
