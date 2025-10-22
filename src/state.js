const OPS_STATUSES = new Set(['DOWNED', 'LIMITED', 'COMFORT', 'COSMETIC', 'HOLD', 'PM', 'VSI']);
const SHOP_STATUSES = new Set(['DOWN', 'UP', 'USABLE']);

function cloneTicket(ticket) {
  return JSON.parse(JSON.stringify(ticket));
}

function vehicleLabel(vehicle) {
  if (!vehicle) return null;
  return vehicle.fleet_no || vehicle.name || vehicle.id;
}

function compareEvents(meta, event) {
  if (!meta) return true;
  if (event.ts > meta.ts) return true;
  if (event.ts < meta.ts) return false;
  return event.event_id > meta.event_id;
}

function ticketMatchesPurge(ticket, vehiclesById, purge) {
  const { dateField, start, end, vehicles } = purge;
  const label = vehicleLabel(vehiclesById.get(ticket.vehicle_id));
  if (vehicles && vehicles.length > 0 && !vehicles.includes(label)) {
    return false;
  }
  const fieldValue = ticket[dateField];
  if (!fieldValue) return false;
  const time = new Date(fieldValue).getTime();
  return time >= purge.start && time <= purge.end;
}

function ensureClosedAt(record) {
  if (!Object.prototype.hasOwnProperty.call(record, 'closed_at')) {
    record.closed_at = record.completed_at || null;
  } else if (record.closed_at == null || record.closed_at === '') {
    record.closed_at = null;
  }
  return record;
}

class DomainState {
  constructor(snapshot) {
    this.vehicles = new Map();
    this.tickets = new Map();
    this.purges = [];
    this.seenEventIds = new Set();
    this.dedupe = new Map();
    this.processedHardPurges = new Set();
    this.lastEventTs = snapshot?.lastEventTs || null;
    if (snapshot) {
      if (snapshot.vehicles) {
        for (const [id, value] of Object.entries(snapshot.vehicles)) {
          this.vehicles.set(id, value);
        }
      }
      if (snapshot.tickets) {
        for (const [id, value] of Object.entries(snapshot.tickets)) {
          const copy = ensureClosedAt({ ...value });
          this.tickets.set(id, copy);
        }
      }
      if (snapshot.purges) {
        this.purges = snapshot.purges;
      }
      if (snapshot.seenEventIds) {
        for (const id of snapshot.seenEventIds) {
          this.seenEventIds.add(id);
        }
      }
      if (snapshot.dedupe) {
        for (const [key, value] of Object.entries(snapshot.dedupe)) {
          this.dedupe.set(key, value);
        }
      }
      if (snapshot.processedHardPurges) {
        for (const id of snapshot.processedHardPurges) {
          this.processedHardPurges.add(id);
        }
      }
    }
  }

  allowedOpsStatus(value) {
    return value == null || OPS_STATUSES.has(value);
  }

  allowedShopStatus(value) {
    return value == null || SHOP_STATUSES.has(value);
  }

  ensureEvent(event) {
    if (this.seenEventIds.has(event.event_id)) {
      return { applied: false, reason: 'duplicate' };
    }
    return { applied: true };
  }

  applyEvent(event) {
    if (this.seenEventIds.has(event.event_id)) {
      return { applied: false, reason: 'duplicate' };
    }

    let dedupeValue;
    let applied = false;
    switch (event.type) {
      case 'vehicle.upserted':
        applied = this.applyVehicle(event);
        if (applied) {
          dedupeValue = { vehicle_id: event.payload.vehicle.id };
        }
        break;
      case 'ticket.created':
      case 'ticket.updated':
      case 'ticket.closed':
        applied = this.applyTicket(event);
        if (applied) {
          dedupeValue = { ticket_id: event.payload.ticket.id };
        }
        break;
      case 'purge.requested':
        applied = this.applyPurge(event);
        if (applied) {
          dedupeValue = { purge_id: event.payload.purge_id };
        }
        break;
      default:
        applied = false;
        break;
    }

    if (applied) {
      this.seenEventIds.add(event.event_id);
      this.lastEventTs = event.ts;
      if (event.dedupe_key) {
        this.dedupe.set(event.dedupe_key, {
          event_id: event.event_id,
          value: dedupeValue,
          ts: event.ts
        });
      }
    }

    return { applied };
  }

  applyVehicle(event) {
    const vehicle = event.payload.vehicle;
    const existing = this.vehicles.get(vehicle.id);
    if (!existing || compareEvents(existing._meta, event)) {
      const copy = { ...vehicle, _meta: { ts: event.ts, event_id: event.event_id } };
      this.vehicles.set(vehicle.id, copy);
      return true;
    }
    return false;
  }

  applyTicket(event) {
    const ticket = event.payload.ticket;
    const existing = this.tickets.get(ticket.id);
    if (!existing || compareEvents(existing._meta, event)) {
      const copy = ensureClosedAt({ ...ticket, _meta: { ts: event.ts, event_id: event.event_id } });
      this.tickets.set(ticket.id, copy);
      return true;
    }
    return false;
  }

  applyPurge(event) {
    const payload = event.payload;
    if (this.processedHardPurges.has(payload.purge_id)) {
      return false;
    }
    const record = {
      purge_id: payload.purge_id,
      start: payload.start,
      end: payload.end,
      dateField: payload.dateField,
      vehicles: payload.vehicles || [],
      mode: payload.mode,
      ts: event.ts
    };
    const existingIndex = this.purges.findIndex((p) => p.purge_id === record.purge_id);
    if (existingIndex >= 0) {
      this.purges[existingIndex] = record;
    } else {
      this.purges.push(record);
    }
    if (record.mode === 'hard') {
      this.processedHardPurges.add(record.purge_id);
    }
    return true;
  }

  serialize() {
    const vehicles = {};
    for (const [id, value] of this.vehicles.entries()) {
      vehicles[id] = value;
    }
    const tickets = {};
    for (const [id, value] of this.tickets.entries()) {
      tickets[id] = value;
    }
    return {
      vehicles,
      tickets,
      purges: this.purges,
      seenEventIds: Array.from(this.seenEventIds),
      dedupe: Object.fromEntries(this.dedupe.entries()),
      processedHardPurges: Array.from(this.processedHardPurges),
      lastEventTs: this.lastEventTs
    };
  }

  getVehicleLabel(vehicleId) {
    return vehicleLabel(this.vehicles.get(vehicleId));
  }

  getTicket(ticketId) {
    return this.tickets.get(ticketId) || null;
  }

  listTickets(options = {}) {
    const includeClosed = options.includeClosed ?? false;
    const filters = options.filters || {};
    const softHidden = this.buildSoftHiddenSet();
    const items = [];
    for (const ticket of this.tickets.values()) {
      if (softHidden.has(ticket.id)) continue;
      if (!includeClosed && ticket.closed_at) continue;
      if (filters.vehicle) {
        const label = this.getVehicleLabel(ticket.vehicle_id);
        if (label !== filters.vehicle) continue;
      }
      if (filters.opsStatus && ticket.ops_status !== filters.opsStatus) continue;
      if (filters.shopStatus && ticket.shop_status !== filters.shopStatus) continue;
      items.push(ticket);
    }
    items.sort((a, b) => {
      const aDate = a.reported_at ? new Date(a.reported_at).getTime() : -Infinity;
      const bDate = b.reported_at ? new Date(b.reported_at).getTime() : -Infinity;
      if (aDate === bDate) {
        if (a.id === b.id) return 0;
        return a.id > b.id ? -1 : 1;
      }
      return bDate - aDate;
    });
    return items.map((t) => this.decorateTicket(t));
  }

  listSignage() {
    const softHidden = this.buildSoftHiddenSet();
    const openTickets = [];
    for (const ticket of this.tickets.values()) {
      if (softHidden.has(ticket.id)) continue;
      if (ticket.closed_at) continue;
      openTickets.push(ticket);
    }
    openTickets.sort((a, b) => {
      const aDate = a.reported_at ? new Date(a.reported_at).getTime() : -Infinity;
      const bDate = b.reported_at ? new Date(b.reported_at).getTime() : -Infinity;
      if (aDate === bDate) {
        if (a.id === b.id) return 0;
        return a.id > b.id ? -1 : 1;
      }
      return bDate - aDate;
    });
    return openTickets.map((ticket) => ({
      ticket_id: ticket.id,
      vehicle: this.getVehicleLabel(ticket.vehicle_id),
      ops_status: ticket.ops_status,
      reported_at: ticket.reported_at,
      reported_by: ticket.reported_by,
      ops_description: ticket.ops_description,
      first_diagnosis_at: ticket.diag_date || ticket.started_at,
      diag_date: ticket.diag_date,
      mechanic: ticket.mechanic,
      diagnosis_text: ticket.diagnosis_text,
      shop_status: ticket.shop_status,
      completed_at: ticket.completed_at
    }));
  }

  buildSoftHiddenSet() {
    const hidden = new Set();
    for (const purge of this.purges) {
      if (!purge || purge.mode === 'hard') continue;
      for (const ticket of this.tickets.values()) {
        if (ticketMatchesPurge(ticket, this.vehicles, purge)) {
          hidden.add(ticket.id);
        }
      }
    }
    return hidden;
  }

  decorateTicket(ticket) {
    const vehicle = this.vehicles.get(ticket.vehicle_id);
    const { _meta, ...rest } = ticket;
    return {
      ...rest,
      vehicle_label: vehicleLabel(vehicle)
    };
  }

  exportTickets({ start, end, includeClosed = true, dateField = 'reported_at' }) {
    const softHidden = this.buildSoftHiddenSet();
    const items = [];
    for (const ticket of this.tickets.values()) {
      if (softHidden.has(ticket.id)) continue;
      const fieldValue = ticket[dateField];
      if (!fieldValue) continue;
      const time = new Date(fieldValue).getTime();
      if (time < start || time > end) continue;
      if (!includeClosed && ticket.closed_at) continue;
      items.push(ticket);
    }
    items.sort((a, b) => {
      const aVehicle = this.getVehicleLabel(a.vehicle_id) || '';
      const bVehicle = this.getVehicleLabel(b.vehicle_id) || '';
      if (aVehicle === bVehicle) {
        const aTime = a[dateField] ? new Date(a[dateField]).getTime() : 0;
        const bTime = b[dateField] ? new Date(b[dateField]).getTime() : 0;
        if (aTime === bTime) {
          return a.id < b.id ? -1 : 1;
        }
        return aTime - bTime;
      }
      return aVehicle < bVehicle ? -1 : 1;
    });
    return items.map((ticket) => this.decorateTicket(ticket));
  }

  getDedupeValue(key) {
    return this.dedupe.get(key) || null;
  }
}

module.exports = {
  DomainState,
  OPS_STATUSES,
  SHOP_STATUSES,
  vehicleLabel,
  ticketMatchesPurge
};
