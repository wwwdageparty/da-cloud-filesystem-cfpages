// ================== Action Handlers ==================

async function handleInit(db: D1Database) {
  const tableSql = `
    CREATE TABLE IF NOT EXISTS ${C_TableName} (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      c1 VARCHAR(255), c2 VARCHAR(255), c3 VARCHAR(255),
      i1 INT, i2 INT, i3 INT,
      d1 DOUBLE, d2 DOUBLE, d3 DOUBLE,
      t1 TEXT, t2 TEXT, t3 TEXT,
      v1 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      v2 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      v3 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`;

  const indices = [
    `CREATE INDEX IF NOT EXISTS idx_${C_TableName}_c1 ON ${C_TableName}(c1)`,
    `CREATE INDEX IF NOT EXISTS idx_${C_TableName}_i1 ON ${C_TableName}(i1)`,
    `CREATE INDEX IF NOT EXISTS idx_${C_TableName}_i2 ON ${C_TableName}(i2)`,
    `CREATE INDEX IF NOT EXISTS idx_${C_TableName}_v1 ON ${C_TableName}(v1)`,
    `CREATE INDEX IF NOT EXISTS idx_${C_TableName}_v2 ON ${C_TableName}(v2)`
  ];

  await db.batch([db.prepare(tableSql), ...indices.map(sql => db.prepare(sql))]);
  return { message: "Filesystem initialized", table: C_TableName };
}

async function handleList(payload: any, db: D1Database) {
  const { results } = await db.prepare(
    `SELECT id, c1 as name, i2 as isFolder, v2 as modified 
     FROM ${C_TableName} WHERE i1 = ? ORDER BY i2 DESC, c1 ASC`
  ).bind(payload.parentId ?? 0).all();
  return { items: results };
}

async function handleRead(payload: any, db: D1Database) {
  const file = await db.prepare(
    `SELECT c1 as name, t1 as content FROM ${C_TableName} WHERE id = ? AND i2 = 0`
  ).bind(payload.id).first();
  if (!file) return { error: "File not found" };
  return file;
}

async function handleWrite(payload: any, db: D1Database) {
  const { id, parentId, name, content, isFolder } = payload;
  if (id) {
    const result = await db.prepare(
      `UPDATE ${C_TableName} SET t1 = ?, v2 = CURRENT_TIMESTAMP WHERE id = ?`
    ).bind(content, id).run();
    return { success: result.success, id };
  } else {
    const type = isFolder ? 1 : 0;
    const info = await db.prepare(
      `INSERT INTO ${C_TableName} (c1, i1, i2, t1) VALUES (?, ?, ?, ?)`
    ).bind(name, parentId || 0, type, content || null).run();
    return { success: true, id: info.meta.last_row_id };
  }
}

async function handleDelete(payload: any, db: D1Database) {
  if (!payload.id) return { error: "Missing ID" };
  const recursiveDeleteSql = `
    DELETE FROM ${C_TableName} WHERE id IN (
      WITH RECURSIVE subordinates AS (
        SELECT id FROM ${C_TableName} WHERE id = ?
        UNION ALL
        SELECT t.id FROM ${C_TableName} t INNER JOIN subordinates s ON t.i1 = s.id
      ) SELECT id FROM subordinates
    );`;
  const result = await db.prepare(recursiveDeleteSql).bind(payload.id).run();
  return { deleted: result.meta.changes ?? 0 };
}

// ================== Core API Router ==================

async function handleApiRequest(action: string, payload: any, env: Env) {
  switch (action) {
    case "init":   return await handleInit(env.DB);
    case "list":   return await handleList(payload, env.DB);
    case "read":   return await handleRead(payload, env.DB);
    case "write":  return await handleWrite(payload, env.DB);
    case "delete": return await handleDelete(payload, env.DB);
    default:       return { error: `Unknown action: ${action}` };
  }
}

// ================== HTTP Wrapper & Helpers ==================

export async function onRequest(context: any) {
  const { request, env, waitUntil } = context;
  if (request.method !== "POST") return new Response("Method Not Allowed", { status: 405 });

  const instanceId = env.DA_INSTANCEID || G_INSTANCE;
  const sourceId = `${C_SERVICE}/${instanceId}`;

  // Auth
  const auth = request.headers.get("Authorization");
  if (!auth || !auth.startsWith("Bearer ") || auth.split(" ")[1] !== env.DA_WRITE_TOKEN) {
    return nack("unknown", sourceId, "UNAUTHORIZED", "Invalid token: " + auth + " " + env.DA_WRITE_TOKEN);
  }

  try {
    const body = (await request.json()) as any;
    const requestId = body.request_id || "unknown";
    if (!body.payload) return nack(requestId, sourceId, "INVALID_FIELD", "Missing payload");

    const ret: any = await handleApiRequest(body.action || "", body.payload, env);
    if (ret && ret.error) return nack(requestId, sourceId, "REQUEST_FAILED", ret.error);

    return ack(requestId, sourceId, ret || {});
  } catch (err: any) {
    return nack("unknown", sourceId, "SYSTEM_ERROR", err.message);
  }
}

function ack(requestId: string, sourceId: string, payload: any = {}) { 
  return new Response(JSON.stringify({ type: "ack", request_id: requestId, source_id: sourceId, payload }, null, 2), 
    { headers: { "Content-Type": "application/json" } }); 
}

function nack(requestId: string, sourceId: string, code: string, message: string) { 
  return new Response(JSON.stringify({ type: "nack", request_id: requestId, source_id: sourceId, payload: { status: "error", code, message } }, null, 2), 
    { status: 400, headers: { "Content-Type": "application/json" } }); 
}

// ================== GLOBALS ==================
interface Env { DB: D1Database; DA_WRITE_TOKEN: string; DA_INSTANCEID?: string; }
const C_TableName = "da_filesystem_root";
const C_SERVICE = "da-cloud-filesystem-cfp";
const C_VERSION = "0.0.1";
let G_INSTANCE = "default";