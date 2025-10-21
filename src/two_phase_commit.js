const { URL } = require('url');
const crypto = require('crypto');

const logger = require('./logger');

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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
    const pendingCommits = new Map();
    let localPrepared = false;
    let commitDecided = false;
    let localCommitPending = false;
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
      commitDecided = true;
      const commitErrors = [];
      for (const entry of remotes) {
        pendingCommits.set(entry.machineId, entry);
      }
      for (const entry of remotes) {
        try {
          await this.sendRemote(entry.peer, 'commit', { transaction_id: txId });
          pendingCommits.delete(entry.machineId);
        } catch (err) {
          commitErrors.push({ peer: entry.peer, machineId: entry.machineId, err });
        }
      }
      localCommitPending = true;
      try {
        await this.storage.commitPreparedTransaction(txId);
        localCommitPending = false;
      } catch (err) {
        commitErrors.push({ peer: null, machineId: this.machineId, err });
      }
      if (commitErrors.length) {
        const aggregate = new Error('two-phase commit incomplete');
        aggregate.commitErrors = commitErrors;
        throw aggregate;
      }
      return {
        transactionId: txId,
        machineIds: Array.from(prepared.keys())
      };
    } catch (err) {
      logger.error('two-phase commit failure', { err: err.message });
      if (!commitDecided) {
        if (localPrepared) {
          await this.storage.rollbackPreparedTransaction(txId).catch(() => {});
        }
        for (const entry of remotes) {
          await this.sendRemote(entry.peer, 'rollback', { transaction_id: txId }).catch(() => {});
        }
      } else {
        await this.finalizeCommit(txId, pendingCommits, localCommitPending);
      }
      throw err;
    }
  }

  async finalizeCommit(txId, pendingRemotes, localCommitPending) {
    let localPending = localCommitPending;
    const remaining = pendingRemotes;
    const retries = [200, 1000, 5000];
    if (!remaining.size && !localPending) {
      return;
    }
    for (const delay of retries) {
      if (localPending) {
        try {
          await this.storage.commitPreparedTransaction(txId);
          localPending = false;
        } catch (err) {
          logger.error('two-phase commit local retry failed', { txId, err: err.message });
        }
      }
      for (const [machineId, entry] of Array.from(remaining.entries())) {
        try {
          await this.sendRemote(entry.peer, 'commit', { transaction_id: txId });
          remaining.delete(machineId);
        } catch (err) {
          logger.error('two-phase commit remote retry failed', {
            txId,
            peer: entry.peer,
            err: err.message
          });
        }
      }
      if (!remaining.size && !localPending) {
        return;
      }
      await wait(delay);
    }
    if (localPending) {
      logger.error('two-phase commit local commit still pending after retries', { txId });
    }
    if (remaining.size) {
      logger.error('two-phase commit remote commits still pending after retries', {
        txId,
        peers: Array.from(remaining.values()).map((entry) => entry.peer)
      });
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
