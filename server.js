const express = require('express');
const path = require('path');
const fsp = require('fs/promises');

const PORT = Number(process.env.PORT || 8080);
const DATA_DIR = process.env.DATA_DIR || '/data';
const SCHEMA_FILE = path.join(DATA_DIR, 'uts_schematic.json');
const TEMP_FILE = path.join(DATA_DIR, 'uts_schematic.json.tmp');

const app = express();
app.use(express.json({ limit: '4mb' }));

let writeLock = false;

async function ensureDataDir() {
  await fsp.mkdir(DATA_DIR, { recursive: true });
}

function defaultSchema() {
  const now = new Date().toISOString();
  return {
    meta: { version: 1, updatedAt: now },
    nodes: {},
    links: [],
    routes: []
  };
}

async function readSchema() {
  await ensureDataDir();
  try {
    const raw = await fsp.readFile(SCHEMA_FILE, 'utf-8');
    const parsed = JSON.parse(raw);
    if (!parsed.meta) {
      parsed.meta = { version: 1, updatedAt: new Date().toISOString() };
    }
    return parsed;
  } catch (err) {
    if (err.code === 'ENOENT') {
      const schema = defaultSchema();
      await writeSchema(schema, true);
      return schema;
    }
    throw err;
  }
}

function validateSchema(schema) {
  const errors = [];
  if (typeof schema !== 'object' || schema === null) {
    return { ok: false, errors: ['Schema must be an object'] };
  }
  if (!schema.meta || typeof schema.meta !== 'object') {
    schema.meta = { version: 1, updatedAt: new Date().toISOString() };
  }
  if (!schema.nodes || typeof schema.nodes !== 'object') {
    errors.push('"nodes" must be an object');
  }
  if (!Array.isArray(schema.links)) {
    errors.push('"links" must be an array');
  }
  if (!Array.isArray(schema.routes)) {
    errors.push('"routes" must be an array');
  }
  if (errors.length) {
    return { ok: false, errors };
  }

  const nodeIds = new Set(Object.keys(schema.nodes));
  for (const [nodeId, node] of Object.entries(schema.nodes)) {
    if (!node || typeof node !== 'object') {
      errors.push(`Node ${nodeId} must be an object`);
      continue;
    }
    if (typeof node.x !== 'number' || typeof node.y !== 'number') {
      errors.push(`Node ${nodeId} must have numeric x/y`);
    }
    if (!node.type || !['stop', 'bend'].includes(node.type)) {
      errors.push(`Node ${nodeId} must have type "stop" or "bend"`);
    }
    if (typeof node.name !== 'string') {
      errors.push(`Node ${nodeId} must have a name`);
    }
  }

  const seenLinkIds = new Set();
  const linkPairs = new Set();
  for (const link of schema.links) {
    if (!link || typeof link !== 'object') {
      errors.push('Link entries must be objects');
      continue;
    }
    if (!link.id || typeof link.id !== 'string') {
      errors.push('Link id must be a string');
    } else if (seenLinkIds.has(link.id)) {
      errors.push(`Duplicate link id ${link.id}`);
    } else {
      seenLinkIds.add(link.id);
    }
    if (!nodeIds.has(link.a) || !nodeIds.has(link.b)) {
      errors.push(`Link ${link.id || '?'} references unknown node`);
    }
    if (link.a === link.b) {
      errors.push(`Link ${link.id || '?'} must connect two different nodes`);
    }
    const key = [link.a, link.b].sort().join('::');
    if (linkPairs.has(key)) {
      errors.push(`Duplicate link between ${link.a} and ${link.b}`);
    } else {
      linkPairs.add(key);
    }
  }

  for (const route of schema.routes) {
    if (!route || typeof route !== 'object') {
      errors.push('Route entries must be objects');
      continue;
    }
    if (!route.id || typeof route.id !== 'string') {
      errors.push('Route id must be a string');
    }
    if (!Array.isArray(route.path)) {
      errors.push(`Route ${route.id || '?'} must have a path array`);
    } else {
      for (const nodeId of route.path) {
        if (!nodeIds.has(nodeId)) {
          errors.push(`Route ${route.id} references missing node ${nodeId}`);
        }
      }
    }
    if (Array.isArray(route.stops)) {
      for (const stop of route.stops) {
        if (!stop || typeof stop !== 'object') {
          errors.push(`Route ${route.id} stop entries must be objects`);
          continue;
        }
        if (stop.node && !nodeIds.has(stop.node)) {
          errors.push(`Route ${route.id} stop references missing node ${stop.node}`);
        }
      }
    } else {
      route.stops = [];
    }
  }

  return { ok: errors.length === 0, errors };
}

async function writeSchema(schema, skipLock = false) {
  if (!skipLock) {
    if (writeLock) {
      throw new Error('WRITE_LOCK');
    }
    writeLock = true;
  }
  try {
    await ensureDataDir();
    const payload = JSON.stringify(schema, null, 2);
    await fsp.writeFile(TEMP_FILE, payload, 'utf-8');
    await fsp.rename(TEMP_FILE, SCHEMA_FILE);
  } finally {
    if (!skipLock) {
      writeLock = false;
    }
  }
}

app.get('/api/schema', async (req, res, next) => {
  try {
    const schema = await readSchema();
    res.json(schema);
  } catch (err) {
    next(err);
  }
});

app.post('/api/schema', async (req, res) => {
  if (writeLock) {
    return res.status(423).json({ ok: false, error: 'Write already in progress' });
  }
  const schema = req.body;
  const validation = validateSchema(schema);
  if (!validation.ok) {
    return res.status(400).json({ ok: false, errors: validation.errors });
  }
  schema.meta = schema.meta || {};
  schema.meta.version = 1;
  schema.meta.updatedAt = new Date().toISOString();
  try {
    await writeSchema(schema);
    res.json({ ok: true, updatedAt: schema.meta.updatedAt });
  } catch (err) {
    if (err.message === 'WRITE_LOCK') {
      res.status(423).json({ ok: false, error: 'Write already in progress' });
    } else {
      console.error('Error writing schema:', err);
      res.status(500).json({ ok: false, error: 'Failed to save schema' });
    }
  }
});

app.post('/api/import-stops', (req, res) => {
  res.status(501).json({ ok: false, error: 'Not implemented' });
});

const rootDir = path.resolve(__dirname);
app.use(express.static(rootDir));

app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ ok: false, error: 'Internal server error' });
});

app.listen(PORT, () => {
  console.log(`UTS schematic server listening on port ${PORT}`);
});
