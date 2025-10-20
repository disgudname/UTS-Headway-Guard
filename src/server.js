const express = require('express');
const crypto = require('crypto');

const config = require('./config');
const { Storage } = require('./storage');
const { Replicator } = require('./replication');
const { OPS_STATUSES, SHOP_STATUSES, ticketMatchesPurge } = require('./state');
const logger = require('./logger');

function createEvent(type, payload, dedupeKey) {
  return {
    event_id: crypto.randomUUID(),
    machine_id: config.machineId,
    ts: new Date().toISOString(),
    type,
    payload,
    ...(dedupeKey ? { dedupe_key: dedupeKey } : {})
  };
}

function parseBoolean(value, defaultValue = false) {
  if (value === undefined) return defaultValue;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    if (value.toLowerCase() === 'true') return true;
    if (value.toLowerCase() === 'false') return false;
  }
  return defaultValue;
}

function parseDate(value) {
  if (!value) return null;
  const date = new Date(value);
  if (isNaN(date.getTime())) return null;
  return date;
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const str = String(value);
  if (/[",\n]/.test(str)) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function buildSignagePage() {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Maintenance Signage</title>
<style>
body { font-family: sans-serif; background: #0b1e39; color: #fff; margin: 0; padding: 20px; }
header { text-align: center; margin-bottom: 20px; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }
.card { background: rgba(255,255,255,0.08); border-radius: 8px; padding: 16px; box-shadow: 0 2px 4px rgba(0,0,0,0.4); }
.card h2 { margin: 0 0 8px 0; font-size: 1.2rem; }
.section { display: flex; justify-content: space-between; gap: 8px; }
.section div { flex: 1; }
.label { font-weight: bold; color: #7cc9ff; }
</style>
</head>
<body>
<header>
<h1>Maintenance Signage</h1>
</header>
<div class="grid" id="cards"></div>
<script>
async function fetchTickets(){
  const res = await fetch('/api/signage');
  if(!res.ok) return;
  const tickets = await res.json();
  const container = document.getElementById('cards');
  container.innerHTML = '';
    tickets.forEach((t) => {
      const el = document.createElement('div');
      el.className = 'card';
      el.innerHTML =
        '<h2>' + (t.vehicle || 'Unknown Vehicle') + '</h2>' +
        '<div class="section">' +
          '<div>' +
            '<div><span class="label">OPS</span>: ' + (t.ops_status || '') + '</div>' +
            '<div><span class="label">Reported</span>: ' + (t.reported_at || '') + '</div>' +
            '<div><span class="label">By</span>: ' + (t.reported_by || '') + '</div>' +
            '<div><span class="label">Description</span>: ' + (t.ops_description || '') + '</div>' +
          '</div>' +
          '<div>' +
            '<div><span class="label">Shop</span>: ' + (t.shop_status || '') + '</div>' +
            '<div><span class="label">Mechanic</span>: ' + (t.mechanic || '') + '</div>' +
            '<div><span class="label">Diagnosis</span>: ' + (t.diagnosis_text || '') + '</div>' +
            '<div><span class="label">Completed</span>: ' + (t.completed_at || '') + '</div>' +
          '</div>' +
        '</div>';
      container.appendChild(el);
    });
}
fetchTickets();
setInterval(fetchTickets, 15000);
</script>
</body>
</html>`;
}

async function bootstrap() {
  const app = express();
  app.use(express.json({ limit: '1mb' }));

  const storage = new Storage(config);
  await storage.init();
  const replicator = new Replicator(storage, config);
  await replicator.start();

  app.locals.storage = storage;
  app.locals.replicator = replicator;

  app.get('/health', (req, res) => {
    res.json({ ok: true });
  });

  app.get('/api/signage', (req, res) => {
    const tickets = storage.state.listSignage();
    res.json(tickets);
  });

  const listTicketsHandler = (req, res) => {
    const includeClosed = parseBoolean(req.query.includeClosed, false);
    const vehicle = req.query.vehicle;
    const opsStatus = req.query.opsStatus;
    const shopStatus = req.query.shopStatus;
    if (opsStatus && !OPS_STATUSES.has(opsStatus)) {
      return res.status(400).json({ error: 'invalid opsStatus' });
    }
    if (shopStatus && !SHOP_STATUSES.has(shopStatus)) {
      return res.status(400).json({ error: 'invalid shopStatus' });
    }
    const tickets = storage.state.listTickets({
      includeClosed,
      filters: { vehicle, opsStatus, shopStatus }
    });
    res.json(tickets);
  };

  app.get('/api/tickets', listTicketsHandler);
  app.get('/tickets', listTicketsHandler);

  const createTicketHandler = async (req, res) => {
    try {
      const body = req.body || {};
      const idempotencyKey = req.get('Idempotency-Key');
      if (idempotencyKey) {
        const existing = storage.state.getDedupeValue(`ticket:create:${idempotencyKey}`);
        if (existing?.value?.ticket_id) {
          const ticket = storage.state.getTicket(existing.value.ticket_id);
          return res.json(storage.state.decorateTicket(ticket));
        }
      }
      const {
        fleet_no,
        vehicle_name,
        vehicle_type,
        reported_at,
        reported_by,
        ops_status,
        ops_description,
        shop_status,
        mechanic,
        diagnosis_text,
        started_at,
        completed_at,
        legacy_row_index,
        legacy_source
      } = body;

      if (!fleet_no && !vehicle_name) {
        return res.status(400).json({ error: 'vehicle identifier required' });
      }

      if (ops_status && !OPS_STATUSES.has(ops_status)) {
        return res.status(400).json({ error: 'invalid ops_status' });
      }
      if (shop_status && !SHOP_STATUSES.has(shop_status)) {
        return res.status(400).json({ error: 'invalid shop_status' });
      }

      const now = new Date().toISOString();
      const vehicle = findOrCreateVehicle(storage, {
        fleet_no: fleet_no || null,
        name: vehicle_name || null,
        vehicle_type: vehicle_type || null
      }, now);

      if (!vehicle) {
        return res.status(400).json({ error: 'vehicle information required' });
      }

      const ticketId = crypto.randomUUID();
      const ticket = {
        id: ticketId,
        vehicle_id: vehicle.id,
        reported_at: reported_at || null,
        reported_by: reported_by || null,
        ops_status: ops_status || null,
        ops_description: ops_description || null,
        shop_status: shop_status || null,
        mechanic: mechanic || null,
        diagnosis_text: diagnosis_text || null,
        started_at: started_at || null,
        completed_at: completed_at || null,
        legacy_row_index: legacy_row_index ?? null,
        legacy_source: legacy_source ?? null,
        created_at: now,
        updated_at: now
      };

      const events = [];
      if (vehicle._emitEvent) {
        events.push(createEvent('vehicle.upserted', { vehicle: vehicle.record }, idempotencyKey ? `vehicle:${idempotencyKey}` : undefined));
      }
      events.push(createEvent('ticket.created', { ticket }, idempotencyKey ? `ticket:create:${idempotencyKey}` : undefined));

      for (const event of events) {
        await storage.appendEvent(event);
      }

      logger.info('ticket created', { ticket_id: ticketId });
      res.status(201).json(storage.state.decorateTicket(storage.state.getTicket(ticketId)));
    } catch (err) {
      logger.error('failed to create ticket', { err: err.stack });
      res.status(500).json({ error: 'internal_error' });
    }
  };

  app.post('/api/tickets', createTicketHandler);
  app.post('/tickets', createTicketHandler);

  const updateTicketHandler = async (req, res) => {
    const ticketId = req.params.id;
    const existing = storage.state.getTicket(ticketId);
    if (!existing) {
      return res.status(404).json({ error: 'ticket_not_found' });
    }
    const body = req.body || {};
    const idempotencyKey = req.get('Idempotency-Key');
    if (idempotencyKey) {
      const dedupe = storage.state.getDedupeValue(`ticket:update:${ticketId}:${idempotencyKey}`);
      if (dedupe?.value?.ticket_id === ticketId) {
        return res.json(storage.state.decorateTicket(existing));
      }
    }
    if (body.ops_status && !OPS_STATUSES.has(body.ops_status)) {
      return res.status(400).json({ error: 'invalid ops_status' });
    }
    if (body.shop_status && !SHOP_STATUSES.has(body.shop_status)) {
      return res.status(400).json({ error: 'invalid shop_status' });
    }
    const now = new Date().toISOString();
    const updated = {
      ...existing,
      ...body,
      updated_at: now
    };
    const eventType = body.completed_at ? 'ticket.closed' : 'ticket.updated';
    const event = createEvent(eventType, { ticket: updated }, idempotencyKey ? `ticket:update:${ticketId}:${idempotencyKey}` : undefined);
    try {
      await storage.appendEvent(event);
      const fresh = storage.state.getTicket(ticketId);
      res.json(storage.state.decorateTicket(fresh));
    } catch (err) {
      logger.error('failed to update ticket', { err: err.stack });
      res.status(500).json({ error: 'internal_error' });
    }
  };

  app.put('/api/tickets/:id', updateTicketHandler);
  app.put('/tickets/:id', updateTicketHandler);

  app.get('/api/export.csv', (req, res) => {
    const start = parseDate(req.query.start);
    const end = parseDate(req.query.end);
    if (!start || !end) {
      return res.status(400).json({ error: 'start and end required' });
    }
    const dateField = req.query.dateField || 'reported_at';
    if (!['reported_at', 'started_at', 'completed_at', 'updated_at'].includes(dateField)) {
      return res.status(400).json({ error: 'invalid dateField' });
    }
    const includeClosed = parseBoolean(req.query.includeClosed, true);
    const items = storage.state.exportTickets({
      start: start.getTime(),
      end: end.getTime(),
      includeClosed,
      dateField
    });
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="export.csv"');
    res.write('vehicle,ticket_id,reported_at,reported_by,ops_status,ops_description,shop_status,mechanic,diagnosis_text,started_at,completed_at,legacy_row_index,legacy_source,created_at,updated_at\n');
    for (const item of items) {
      const line = [
        csvEscape(item.vehicle_label || ''),
        csvEscape(item.id),
        csvEscape(item.reported_at || ''),
        csvEscape(item.reported_by || ''),
        csvEscape(item.ops_status || ''),
        csvEscape(item.ops_description || ''),
        csvEscape(item.shop_status || ''),
        csvEscape(item.mechanic || ''),
        csvEscape(item.diagnosis_text || ''),
        csvEscape(item.started_at || ''),
        csvEscape(item.completed_at || ''),
        csvEscape(item.legacy_row_index == null ? '' : item.legacy_row_index),
        csvEscape(item.legacy_source || ''),
        csvEscape(item.created_at || ''),
        csvEscape(item.updated_at || '')
      ].join(',');
      res.write(`${line}\n`);
    }
    res.end();
  });

  app.post('/api/purge', async (req, res) => {
    const body = req.body || {};
    const startDate = parseDate(body.start);
    const endDate = parseDate(body.end);
    if (!startDate || !endDate) {
      return res.status(400).json({ error: 'invalid start or end' });
    }
    const dateField = body.dateField || 'reported_at';
    if (!['reported_at', 'started_at', 'completed_at', 'updated_at'].includes(dateField)) {
      return res.status(400).json({ error: 'invalid dateField' });
    }
    const vehicles = Array.isArray(body.vehicles) ? body.vehicles.filter(Boolean) : [];
    const purgeId = crypto.randomUUID();
    const purgeSpec = {
      purge_id: purgeId,
      start: startDate.getTime(),
      end: endDate.getTime(),
      dateField,
      vehicles,
      mode: body.hard ? 'hard' : 'soft'
    };
    const purgedTickets = [];
    for (const ticket of storage.state.tickets.values()) {
      if (ticketMatchesPurge(ticket, storage.state.vehicles, purgeSpec)) {
        purgedTickets.push(ticket.id);
      }
    }
    const event = createEvent('purge.requested', purgeSpec);
    try {
      await storage.appendEvent(event);
      if (purgeSpec.mode === 'hard') {
        await storage.performHardPurge(purgeSpec);
      }
      res.json({ purged_count: purgedTickets.length, mode: purgeSpec.mode, purge_id: purgeId });
    } catch (err) {
      logger.error('purge failed', { err: err.stack });
      res.status(500).json({ error: 'internal_error' });
    }
  });

  app.post('/internal/replicate', async (req, res) => {
    const body = req.body || {};
    if (!Array.isArray(body.events)) {
      return res.status(400).json({ error: 'events array required' });
    }
    const applied = [];
    for (const event of body.events) {
      const result = await storage.persistIncomingEvent(event);
      if (result.applied) {
        applied.push(event.event_id);
        if (event.type === 'purge.requested' && event.payload?.mode === 'hard') {
          await storage.performHardPurge(event.payload);
        }
      }
    }
    res.json({ ack: true, machine_id: config.machineId, applied_event_ids: applied });
  });

  app.get('/internal/export', async (req, res) => {
    const since = req.query.since_ts || null;
    const events = await storage.eventsSince(since);
    res.json(events);
  });

  app.get('/signage', (req, res) => {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(buildSignagePage());
  });

  app.use((err, req, res, next) => {
    logger.error('unhandled error', { err: err.stack });
    res.status(500).json({ error: 'internal_error' });
  });

  const server = app.listen(config.port, () => {
    logger.info('server listening', { port: config.port, machine_id: config.machineId });
  });

  const shutdown = async () => {
    logger.info('shutting down');
    replicator.stop();
    clearInterval(storage.snapshotTimer);
    if (storage.eventHandle) {
      await storage.eventHandle.close().catch(() => {});
    }
    server.close(() => process.exit(0));
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

function findOrCreateVehicle(storage, details, now) {
  const state = storage.state;
  let existing = null;
  for (const vehicle of state.vehicles.values()) {
    if (details.fleet_no && vehicle.fleet_no === details.fleet_no) {
      existing = vehicle;
      break;
    }
    if (!existing && details.name && vehicle.name === details.name) {
      existing = vehicle;
    }
  }
  if (existing) {
    const needsUpdate =
      existing.fleet_no !== details.fleet_no ||
      existing.name !== details.name ||
      existing.vehicle_type !== details.vehicle_type;
    if (needsUpdate) {
      const updated = {
        ...existing,
        fleet_no: details.fleet_no || existing.fleet_no,
        name: details.name || existing.name,
        vehicle_type: details.vehicle_type || existing.vehicle_type,
        active: true,
        updated_at: now
      };
      delete updated._meta;
      return { id: existing.id, record: updated, _emitEvent: true };
    }
    const existingCopy = { ...existing };
    delete existingCopy._meta;
    return { id: existing.id, record: existingCopy, _emitEvent: false };
  }
  const id = crypto.randomUUID();
  const vehicle = {
    id,
    fleet_no: details.fleet_no || null,
    name: details.name || null,
    vehicle_type: details.vehicle_type || null,
    active: true,
    created_at: now,
    updated_at: now
  };
  return { id, record: vehicle, _emitEvent: true };
}

bootstrap().catch((err) => {
  logger.error('failed to start server', { err: err.stack });
  process.exit(1);
});
