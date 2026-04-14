/**
 * Suluk Worker — Personal Content Library + Data Backup + AI Chat
 *
 * R2 bucket: suluk-personal (private)
 * Auth: Bearer token stored in env.AUTH_TOKEN
 * AI:   Gemini API key stored in env.GEMINI_API_KEY
 *
 * Routes:
 *   POST   /upload         — upload a file (multipart/form-data)
 *   GET    /file/:key      — stream a file
 *   DELETE /file/:key      — delete a file
 *   GET    /files          — list all files (for rebuilding index)
 *   POST   /backup-data    — store a JSON backup of localStorage data
 *   GET    /backup-data    — retrieve the latest backup
 *   POST   /chat           — proxy chat messages to Gemini 2.5 Pro
 *   POST   /rag-ingest     — (admin) embed+upsert chunks into Vectorize
 *   POST   /rag-search     — (admin) vector search over the knowledge index
 *   POST   /rag-chat       — (admin) RAG chat: search corpus + Gemini answer
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

      // Chat endpoint is public (no auth required)
      if (request.method === 'POST' && path === '/chat') {
        return handleChat(request, env, corsOrigin);
      }

      // RAG search is public-readable (same as /chat) so the Study Companion
      // can call it without requiring an admin token from the browser.
      if (request.method === 'POST' && path === '/rag-search') {
        return handleRagSearch(request, env, corsOrigin);
      }
      if (request.method === 'POST' && path === '/rag-chat') {
        return handleRagChat(request, env, corsOrigin);
      }

      // Auth check (all other routes require token)
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

      // POST /rag-ingest (admin) — embed + upsert chunks into Vectorize
      if (request.method === 'POST' && path === '/rag-ingest') {
        return handleRagIngest(request, env, corsOrigin);
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


// ─── AI Chat (Gemini 2.5 Pro) ────────────────────────────
async function handleChat(request, env, corsOrigin) {
  const { messages, teachingContext, handbookContext, glossaryContext, kabirContext } = await request.json();

  if (!env.GEMINI_API_KEY) {
    return jsonResponse(500, { error: 'Gemini API key not configured' }, corsOrigin);
  }

  if (!messages || !messages.length) {
    return jsonResponse(400, { error: 'No messages provided' }, corsOrigin);
  }

  // Build context from handbook teachings
  let teachingExcerpts = '';
  if (handbookContext && handbookContext.length) {
    teachingExcerpts = '\n\n--- RELEVANT TEACHINGS FROM THE HANDBOOK ---\n';
    handbookContext.forEach((t, i) => {
      teachingExcerpts += `\n[${i + 1}] "${t.title}" (${t.chapter}${t.instructor ? ', ' + t.instructor : ''})\n${t.body}\n`;
    });
  }

  let currentTeaching = '';
  if (teachingContext) {
    currentTeaching = `\n\n--- CURRENTLY READING ---\nTitle: ${teachingContext.title}\nChapter: ${teachingContext.chapter}\nInstructor: ${teachingContext.instructor || ''}\nContent: ${teachingContext.body?.slice(0, 2000) || ''}`;
  }

  const systemPrompt = `You are a study companion for the Suluk Digital Handbook — a personal learning companion for the Inayatiyya Suluk Academy teachings.

IMPORTANT RULES:
1. Base your answers on the handbook teachings AND Kabir's personal writings provided below. Both are valid primary sources.
2. When relevant information is present, quote or reference the specific teaching titles or reflection titles (e.g., "From Kabir's reflection 'Air and Water'…" or "From the chapter on Concentration…").
3. You may provide brief contextual explanations to help the student understand Arabic/Persian terms and Sufi concepts mentioned.
4. If NEITHER the teachings NOR Kabir's writings contain relevant information at all, say: "I couldn't find that in the handbook or in Kabir's writings. Try rephrasing your question or exploring the Search and Glossary features."
5. Always indicate which source(s) your answer draws from.
6. Kabir's writings are personal contemplative reflections — treat them with the same reverence as the handbook teachings.

Your role:
- Explain and reflect on the teachings provided in the context below
- Draw connections between the provided teachings
- Help the student understand concepts, practices, and Arabic/Persian terms found in the handbook
- Be respectful and reverent toward the teachings and the Inayatiyya lineage
- When discussing practices, note that proper guidance from a teacher is important
- Keep responses focused and grounded in the actual handbook content
- Reference specific teaching titles when quoting or paraphrasing
- If the student is currently reading a teaching, prioritize that context` + currentTeaching + teachingExcerpts +
    (kabirContext && kabirContext.length ? '\n\n--- KABIR\'S WRITINGS (personal reflections) ---\n' + kabirContext.map((k, i) => `\n[K${i + 1}] "${k.title}" (${k.source || "Kabir's Writings"})\n${k.body}\n`).join('') : '') +
    (glossaryContext && glossaryContext.length ? '\n\n--- GLOSSARY TERMS ---\n' + glossaryContext.map(g => `- **${g.term}**: ${g.definition}`).join('\n') : '');

  // Build Gemini API request
  const geminiMessages = messages.map(m => ({
    role: m.role === 'assistant' ? 'model' : 'user',
    parts: [{ text: m.content }],
  }));

  const geminiBody = {
    system_instruction: { parts: [{ text: systemPrompt }] },
    contents: geminiMessages,
    generationConfig: {
      temperature: 0.7,
      maxOutputTokens: 2048,
    },
  };

  const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${env.GEMINI_API_KEY}`;

  const geminiResp = await fetch(geminiUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(geminiBody),
  });

  if (!geminiResp.ok) {
    const errText = await geminiResp.text();
    return jsonResponse(geminiResp.status, { error: 'Gemini API error', details: errText }, corsOrigin);
  }

  const geminiData = await geminiResp.json();
  const reply = geminiData.candidates?.[0]?.content?.parts?.[0]?.text || 'No response generated.';

  return jsonResponse(200, { reply }, corsOrigin);
}


// ─── RAG: Ingest (admin) ─────────────────────────────────
// POST /rag-ingest
// Body: { chunks: [{ id, text, metadata }, ...] }
// Max batch size: 100 (Vectorize limit). Caller should batch ~50 at a time
// to stay under the 10MB request limit and Workers AI rate limits.
async function handleRagIngest(request, env, corsOrigin) {
  if (!env.VECTORIZE || !env.AI) {
    return jsonResponse(500, { error: 'Vectorize or AI binding missing' }, corsOrigin);
  }
  const { chunks } = await request.json();
  if (!Array.isArray(chunks) || chunks.length === 0) {
    return jsonResponse(400, { error: 'No chunks provided' }, corsOrigin);
  }
  if (chunks.length > 100) {
    return jsonResponse(400, { error: 'Batch too large (max 100)' }, corsOrigin);
  }

  // Embed all texts in a single Workers AI call (BGE supports batch input)
  const texts = chunks.map(c => String(c.text || ''));
  let embedResult;
  try {
    embedResult = await env.AI.run('@cf/baai/bge-base-en-v1.5', { text: texts });
  } catch (aiErr) {
    return jsonResponse(503, {
      error: 'Workers AI embedding failed',
      detail: String(aiErr.message || aiErr),
    }, corsOrigin);
  }
  const vectors = embedResult.data || embedResult.embeddings || [];
  if (vectors.length !== chunks.length) {
    return jsonResponse(500, {
      error: 'Embedding count mismatch',
      expected: chunks.length,
      got: vectors.length,
    }, corsOrigin);
  }

  // Build Vectorize upsert payload
  const toUpsert = chunks.map((c, i) => ({
    id: String(c.id),
    values: vectors[i],
    metadata: c.metadata || {},
  }));

  let result;
  try {
    result = await env.VECTORIZE.upsert(toUpsert);
  } catch (vecErr) {
    return jsonResponse(503, {
      error: 'Vectorize upsert failed',
      detail: String(vecErr.message || vecErr),
    }, corsOrigin);
  }
  return jsonResponse(200, {
    ok: true,
    upserted: toUpsert.length,
    mutationId: result.mutationId || null,
  }, corsOrigin);
}


// ─── RAG: Search ─────────────────────────────────────────
// POST /rag-search
// Body: { query, topK?, namespace?, book?, author? }
async function handleRagSearch(request, env, corsOrigin) {
  if (!env.VECTORIZE || !env.AI) {
    return jsonResponse(500, { error: 'Vectorize or AI binding missing' }, corsOrigin);
  }
  const body = await request.json().catch(() => ({}));
  const query = (body.query || '').trim();
  if (!query) {
    return jsonResponse(400, { error: 'Missing query' }, corsOrigin);
  }
  const topK = Math.min(Math.max(parseInt(body.topK || 8, 10), 1), 25);

  // Build metadata filter (optional)
  const filter = {};
  if (body.namespace) filter.namespace = { $eq: body.namespace };
  if (body.book) filter.book = { $eq: body.book };
  if (body.author) filter.author = { $eq: body.author };

  // Embed the query
  let embed;
  try {
    embed = await env.AI.run('@cf/baai/bge-base-en-v1.5', { text: [query] });
  } catch (aiErr) {
    return jsonResponse(503, {
      error: 'Workers AI embedding failed (quota may be exhausted)',
      detail: String(aiErr.message || aiErr),
    }, corsOrigin);
  }
  const qVec = (embed.data || embed.embeddings || [])[0];
  if (!qVec) {
    return jsonResponse(500, { error: 'Failed to embed query' }, corsOrigin);
  }

  // Vector search
  const searchOpts = {
    topK,
    returnMetadata: 'all',
    returnValues: false,
  };
  if (Object.keys(filter).length > 0) searchOpts.filter = filter;

  let result;
  try {
    result = await env.VECTORIZE.query(qVec, searchOpts);
  } catch (vecErr) {
    return jsonResponse(503, {
      error: 'Vectorize query failed',
      detail: String(vecErr.message || vecErr),
    }, corsOrigin);
  }
  const matches = (result.matches || []).map(m => ({
    id: m.id,
    score: m.score,
    ...m.metadata,
  }));

  return jsonResponse(200, {
    query,
    count: matches.length,
    matches,
  }, corsOrigin);
}


// ─── RAG: Chat (search + Gemini) ─────────────────────────
// POST /rag-chat
// Body: { messages, namespace?, topK?, teachingContext?, handbookContext?, kabirContext?, glossaryContext? }
async function handleRagChat(request, env, corsOrigin) {
  if (!env.VECTORIZE || !env.AI || !env.GEMINI_API_KEY) {
    return jsonResponse(500, { error: 'RAG chat requires Vectorize, AI, and Gemini keys' }, corsOrigin);
  }
  const body = await request.json();
  const { messages, teachingContext, handbookContext, glossaryContext, kabirContext } = body;
  // namespace: 'sufi-library' (public, default), 'hik'/'hik-online'/'ruhaniat'/'sufi-message'/'sufi-canada' (admin), 'all' (admin), 'none' (skip vector search)
  const namespace = body.namespace || 'sufi-library';
  const ADMIN_NAMESPACES = ['hik', 'hik-online', 'ruhaniat', 'sufi-message', 'sufi-canada', 'all'];
  if (!messages || !messages.length) {
    return jsonResponse(400, { error: 'No messages provided' }, corsOrigin);
  }

  // Server-side admin gate for non-public namespaces
  if (ADMIN_NAMESPACES.includes(namespace)) {
    const authHeader = request.headers.get('Authorization') || '';
    const token = authHeader.replace('Bearer ', '');
    if (!token || token !== env.AUTH_TOKEN) {
      return jsonResponse(401, { error: 'HIK Library requires admin access' }, corsOrigin);
    }
  }

  // Use the latest user message as the retrieval query
  const lastUser = [...messages].reverse().find(m => m.role === 'user');
  const retrievalQuery = (lastUser?.content || '').trim();

  // Run vector search against the corpus (skip when namespace='none')
  let corpusMatches = [];
  if (retrievalQuery && namespace !== 'none') {
    const embed = await env.AI.run('@cf/baai/bge-base-en-v1.5', { text: [retrievalQuery] });
    const qVec = (embed.data || embed.embeddings || [])[0];
    if (qVec) {
      const searchOpts = {
        topK: 10,
        returnMetadata: 'all',
        returnValues: false,
      };
      // 'all' = no namespace filter; otherwise filter to the requested namespace
      if (namespace !== 'all') searchOpts.filter = { namespace: { $eq: namespace } };
      try {
        const res = await env.VECTORIZE.query(qVec, searchOpts);
        corpusMatches = (res.matches || []).map(m => ({
          score: m.score,
          ...m.metadata,
        }));
      } catch (e) {
        // If Vectorize query fails, degrade gracefully to normal chat
        corpusMatches = [];
      }
    }
  }

  // Build corpus excerpts block
  let corpusExcerpts = '';
  if (corpusMatches.length) {
    corpusExcerpts = '\n\n--- RELEVANT PASSAGES FROM THE KNOWLEDGE LIBRARY ---\n';
    corpusMatches.forEach((m, i) => {
      const cite = [m.book, m.chapter_title].filter(Boolean).join(' — ');
      corpusExcerpts += `\n[L${i + 1}] ${cite}${m.page_start ? ` (p. ${m.page_start})` : ''}\n${m.text || ''}\n`;
    });
  }

  // Build handbook excerpts (existing behaviour preserved)
  let teachingExcerpts = '';
  if (handbookContext && handbookContext.length) {
    teachingExcerpts = '\n\n--- RELEVANT TEACHINGS FROM THE HANDBOOK ---\n';
    handbookContext.forEach((t, i) => {
      teachingExcerpts += `\n[H${i + 1}] "${t.title}" (${t.chapter}${t.instructor ? ', ' + t.instructor : ''})\n${t.body}\n`;
    });
  }

  let currentTeaching = '';
  if (teachingContext) {
    currentTeaching = `\n\n--- CURRENTLY READING ---\nTitle: ${teachingContext.title}\nChapter: ${teachingContext.chapter}\nInstructor: ${teachingContext.instructor || ''}\nContent: ${teachingContext.body?.slice(0, 2000) || ''}`;
  }

  const systemPrompt = `You are a study companion for a Suluk Academy practitioner with deep access to three knowledge sources:

1. The Suluk Digital Handbook (your primary Inayatiyya teaching reference)
2. Kabir's personal contemplative writings (reflections, Crimson Heart, Light Dreaming, Journey of Light)
3. The Knowledge Library — Hazrat Inayat Khan's Complete Works + other Sufi literature

IMPORTANT RULES:
1. Ground every answer in the provided passages. Quote or paraphrase and cite the source.
2. Citation format: use the bracket tags from the context ([L1], [H1], [K1]) followed by the source name. Example: "[L3] The Inner Life, ch. 4".
3. When a passage from the Knowledge Library (L) directly answers the question, lead with that. The library is the most authoritative source for HIK's own words.
4. When Kabir's writings (K) add a personal or reflective angle, weave them in.
5. When the Handbook (H) applies, use it for Suluk Academy specific framing.
6. If NONE of the provided passages are relevant, say so plainly — do not invent.
7. Be respectful and reverent toward the teachings and the Inayatiyya lineage.
8. When discussing practices, note that proper guidance from a teacher is important.` + currentTeaching + corpusExcerpts + teachingExcerpts +
    (kabirContext && kabirContext.length ? '\n\n--- KABIR\'S WRITINGS (personal reflections) ---\n' + kabirContext.map((k, i) => `\n[K${i + 1}] "${k.title}" (${k.source || "Kabir's Writings"})\n${k.body}\n`).join('') : '') +
    (glossaryContext && glossaryContext.length ? '\n\n--- GLOSSARY TERMS ---\n' + glossaryContext.map(g => `- **${g.term}**: ${g.definition}`).join('\n') : '');

  const geminiMessages = messages.map(m => ({
    role: m.role === 'assistant' ? 'model' : 'user',
    parts: [{ text: m.content }],
  }));

  const geminiBody = {
    system_instruction: { parts: [{ text: systemPrompt }] },
    contents: geminiMessages,
    generationConfig: { temperature: 0.6, maxOutputTokens: 2048 },
  };

  // Try models in order: Flash (preferred) → Flash-Lite → 1.5 Flash
  // Each model gets one retry on 503. This absorbs transient Google demand spikes.
  const modelChain = ['gemini-2.5-flash', 'gemini-2.5-flash-lite', 'gemini-1.5-flash'];
  let geminiData = null;
  let lastErrText = '';
  let lastStatus = 500;
  let modelUsed = null;

  outer: for (const model of modelChain) {
    for (let attempt = 1; attempt <= 2; attempt++) {
      const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${env.GEMINI_API_KEY}`;
      const geminiResp = await fetch(geminiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(geminiBody),
      });
      if (geminiResp.ok) {
        geminiData = await geminiResp.json();
        modelUsed = model;
        break outer;
      }
      lastStatus = geminiResp.status;
      lastErrText = await geminiResp.text();
      // Fall through on transient 5xx (overload, unavailable) OR 429 (free-tier quota)
      // Hard 4xx (400, 401, 403, etc.) are real errors — don't retry.
      const isRetryable = geminiResp.status === 429 || (geminiResp.status >= 500 && geminiResp.status !== 501);
      if (!isRetryable) break outer;
      // For 429 quota errors, skip straight to the next model (same model won't recover)
      if (geminiResp.status === 429) break;
      // For 5xx, short backoff before retrying the same model
      if (attempt === 1) await new Promise(r => setTimeout(r, 500));
    }
  }

  if (!geminiData) {
    return jsonResponse(lastStatus, { error: 'Gemini API error', details: lastErrText }, corsOrigin);
  }

  const reply = geminiData.candidates?.[0]?.content?.parts?.[0]?.text || 'No response generated.';

  return jsonResponse(200, {
    reply,
    modelUsed,
    corpusMatches: corpusMatches.slice(0, 10).map(m => ({
      book: m.book,
      author: m.author,
      chapter: m.chapter,
      chapter_title: m.chapter_title,
      page_start: m.page_start,
      namespace: m.namespace,
      score: m.score,
    })),
  }, corsOrigin);
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
