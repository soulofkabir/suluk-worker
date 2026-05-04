/**
 * Suluk Worker — Personal Content Library + Data Backup
 *
 * R2 bucket: suluk-personal (private)
 * Auth: Bearer token stored in env.AUTH_TOKEN
 *
 * Routes:
 *   POST   /upload         — upload a file (multipart/form-data)
 *   GET    /file/:key      — stream a file
 *   DELETE /file/:key      — delete a file
 *   GET    /files          — list all files (for rebuilding index)
 *   POST   /backup-data    — store a JSON backup of localStorage data
 *   GET    /backup-data    — retrieve the latest backup
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const requestOrigin = request.headers.get('Origin') || '';
    const allowedOrigins = [
      env.CORS_ORIGIN || 'https://soulofkabir.github.io',
      'http://localhost:7777',
      'http://localhost:8080',
      'http://127.0.0.1:7777',
      'http://127.0.0.1:8080',
    ];
    const corsOrigin = allowedOrigins.includes(requestOrigin) ? requestOrigin : allowedOrigins[0];

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: corsHeaders(corsOrigin),
      });
    }

    try {
      const path = url.pathname;

      // Auth check (all routes require token)
      const authHeader = request.headers.get('Authorization') || '';
      const token = authHeader.replace('Bearer ', '');
      if (!token || token !== env.AUTH_TOKEN) {
        return jsonResponse(401, { error: 'Unauthorized' }, corsOrigin);
      }

      // POST /upload
      if (request.method === 'POST' && path === '/upload') {
        return handleUpload(request, env, corsOrigin);
      }

      // GET /files — list all objects
      if (request.method === 'GET' && path === '/files') {
        return handleList(env, corsOrigin);
      }

      // POST /backup-data — store JSON backup
      if (request.method === 'POST' && path === '/backup-data') {
        return handleBackupSave(request, env, corsOrigin);
      }

      // GET /backup-data — retrieve JSON backup
      if (request.method === 'GET' && path === '/backup-data') {
        return handleBackupLoad(env, corsOrigin);
      }

      // GET /file/:key — stream file
      if (request.method === 'GET' && path.startsWith('/file/')) {
        const key = decodeURIComponent(path.slice(6));
        return handleDownload(key, env, corsOrigin);
      }

      // DELETE /file/:key — delete file
      if (request.method === 'DELETE' && path.startsWith('/file/')) {
        const key = decodeURIComponent(path.slice(6));
        return handleDelete(key, env, corsOrigin);
      }

      return jsonResponse(404, { error: 'Not found' }, corsOrigin);

    } catch (err) {
      return jsonResponse(500, { error: err.message }, corsOrigin);
    }
  },
};


// ─── Upload ──────────────────────────────────────────────
async function handleUpload(request, env, corsOrigin) {
  const formData = await request.formData();
  const file = formData.get('file');
  if (!file) {
    return jsonResponse(400, { error: 'No file provided' }, corsOrigin);
  }

  const category = formData.get('category') || 'general';
  const timestamp = Date.now();
  const safeName = file.name.replace(/[^a-zA-Z0-9._-]/g, '_');
  const key = `${category}/${timestamp}_${safeName}`;

  await env.PERSONAL_BUCKET.put(key, file.stream(), {
    httpMetadata: { contentType: file.type },
    customMetadata: {
      originalName: file.name,
      category: category,
      uploadedAt: new Date().toISOString(),
      size: String(file.size),
    },
  });

  return jsonResponse(200, {
    success: true,
    key: key,
    name: file.name,
    size: file.size,
    type: file.type,
    category: category,
  }, corsOrigin);
}


// ─── Download / Stream ───────────────────────────────────
async function handleDownload(key, env, corsOrigin) {
  const object = await env.PERSONAL_BUCKET.get(key);
  if (!object) {
    return jsonResponse(404, { error: 'File not found' }, corsOrigin);
  }

  const headers = {
    ...corsHeaders(corsOrigin),
    'Content-Type': object.httpMetadata?.contentType || 'application/octet-stream',
    'Content-Length': object.size,
    'Cache-Control': 'private, max-age=3600',
  };

  // Add original filename for downloads
  const originalName = object.customMetadata?.originalName;
  if (originalName) {
    headers['Content-Disposition'] = `inline; filename="${originalName}"`;
  }

  return new Response(object.body, { headers });
}


// ─── Delete ──────────────────────────────────────────────
async function handleDelete(key, env, corsOrigin) {
  await env.PERSONAL_BUCKET.delete(key);
  return jsonResponse(200, { success: true, key }, corsOrigin);
}


// ─── List all files ──────────────────────────────────────
async function handleList(env, corsOrigin) {
  const files = [];
  let cursor = undefined;
  let truncated = true;

  while (truncated) {
    const listed = await env.PERSONAL_BUCKET.list({
      cursor,
      limit: 500,
      include: ['httpMetadata', 'customMetadata'],
    });

    for (const obj of listed.objects) {
      files.push({
        key: obj.key,
        size: obj.size,
        uploaded: obj.uploaded,
        type: obj.httpMetadata?.contentType || 'unknown',
        name: obj.customMetadata?.originalName || obj.key.split('/').pop(),
        category: obj.customMetadata?.category || 'general',
      });
    }

    truncated = listed.truncated;
    cursor = listed.cursor;
  }

  return jsonResponse(200, { files, count: files.length }, corsOrigin);
}


// ─── Backup save ─────────────────────────────────────────
async function handleBackupSave(request, env, corsOrigin) {
  const data = await request.text();

  // Validate it's valid JSON
  try { JSON.parse(data); } catch {
    return jsonResponse(400, { error: 'Invalid JSON' }, corsOrigin);
  }

  const key = '_backups/suluk_user_data.json';
  await env.PERSONAL_BUCKET.put(key, data, {
    httpMetadata: { contentType: 'application/json' },
    customMetadata: { backedUpAt: new Date().toISOString() },
  });

  return jsonResponse(200, { success: true, backedUpAt: new Date().toISOString() }, corsOrigin);
}


// ─── Backup load ─────────────────────────────────────────
async function handleBackupLoad(env, corsOrigin) {
  const key = '_backups/suluk_user_data.json';
  const object = await env.PERSONAL_BUCKET.get(key);
  if (!object) {
    return jsonResponse(404, { error: 'No backup found' }, corsOrigin);
  }

  const data = await object.text();
  return new Response(data, {
    headers: {
      ...corsHeaders(corsOrigin),
      'Content-Type': 'application/json',
    },
  });
}


// ─── Helpers ─────────────────────────────────────────────
function corsHeaders(origin) {
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Authorization, Content-Type',
    'Access-Control-Max-Age': '86400',
  };
}

function jsonResponse(status, body, corsOrigin) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      ...corsHeaders(corsOrigin),
      'Content-Type': 'application/json',
    },
  });
}
