const { URL } = require('url');
const crypto = require('crypto');

const logger = require('./logger');

class TwoPhaseCoordinator {
  constructor(storage, cfg) {
    this.storage = storage;
    this.config = cfg;
    this.machineId = cfg.machineId;
    this.peers = cfg.peers || [];
    const ids = Array.isArray(cfg.requiredMachineIds) ? cfg.requiredMachineIds : [];
    this.requiredMachineIds = new Set(ids.filter((id) => typeof id === 'string' && id.trim()));
  }

  resolveRequired(prepared) {
    if (!this.peers.length) {
      return new Set([this.machineId]);
    }
    if (this.requiredMachineIds.size) {
      return this.requiredMachineIds;
    }
    return new Set(prepared.keys());
  }

  async commit(actions) {
    if (!Array.isArray(actions) || actions.length === 0) {
      throw new Error('actions required for two-phase commit');
    }
    const txId = `tx-${crypto.randomUUID()}`;
    const prepared = new Map();
    const remotes = [];
    let localPrepared = false;
    try {
      await this.storage.prepareTransaction(txId, actions);
      localPrepared = true;
      prepared.set(this.machineId, { type: 'local' });
      for (const peer of this.peers) {
        const data = await this.beginRemote(peer, txId, actions);
        const machineId = data.machine_id;
        if (!machineId) {
          throw new Error(`peer ${peer} did not supply machine_id`);
        }
        prepared.set(machineId, { type: 'remote', peer });
        remotes.push({ peer, machineId });
      }
      const required = this.resolveRequired(prepared);
      for (const id of required) {
        if (!prepared.has(id)) {
          throw new Error(`missing prepare acknowledgement from machine ${id}`);
        }
      }
      for (const entry of remotes) {
        await this.sendRemote(entry.peer, 'commit', { transaction_id: txId });
      }
      await this.storage.commitPreparedTransaction(txId);
      return {
        transactionId: txId,
        machineIds: Array.from(prepared.keys())
      };
    } catch (err) {
      logger.error('two-phase commit failure', { err: err.message });
      if (localPrepared) {
        await this.storage.rollbackPreparedTransaction(txId).catch(() => {});
      }
      for (const entry of remotes) {
        await this.sendRemote(entry.peer, 'rollback', { transaction_id: txId }).catch(() => {});
      }
      throw err;
    }
  }

  async beginRemote(peer, txId, actions) {
    const url = new URL('/internal/2pc/begin', peer).toString();
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ transaction_id: txId, actions }),
      signal: AbortSignal.timeout(10000)
    });
    if (!res.ok) {
      throw new Error(`peer ${peer} begin failed with status ${res.status}`);
    }
    try {
      return await res.json();
    } catch (err) {
      logger.error('failed parsing peer begin response', { peer, err: err.message });
      return {};
    }
  }

  async sendRemote(peer, phase, payload) {
    const url = new URL(`/internal/2pc/${phase}`, peer).toString();
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(10000)
    });
    if (!res.ok) {
      throw new Error(`peer ${peer} ${phase} failed with status ${res.status}`);
    }
    try {
      return await res.json();
    } catch (err) {
      return {};
    }
  }
}

module.exports = {
  TwoPhaseCoordinator
};
