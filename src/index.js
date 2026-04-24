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
  // namespace: 'sufi-library' (public, default), 'hik'/'hik-online'/'ruhaniat'/'sufi-message'/'sufi-canada'/'hik-message'/'pir-vilayat'/'suluk-classes' (admin), 'hik-all' (admin, HIK-only compound: hik+hik-online+hik-message), 'all' (admin), 'none' (skip vector search)
  const namespace = body.namespace || 'sufi-library';
  const ADMIN_NAMESPACES = ['hik', 'hik-online', 'ruhaniat', 'sufi-message', 'sufi-canada', 'hik-message', 'pir-vilayat', 'suluk-classes', 'hik-all', 'all'];
  // Compound namespace: hik-all restricts to the three HIK vector namespaces only.
  const HIK_ONLY = namespace === 'hik-all';
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

  // Build the vector search filter once
  const buildFilter = () => {
    if (HIK_ONLY) return { namespace: { $in: ['hik', 'hik-online', 'hik-message'] } };
    if (namespace !== 'all') return { namespace: { $eq: namespace } };
    return undefined;
  };

  // Multi-query retrieval: expand the user's question into 3-4 related search
  // queries, then union the results. This rescues recall for procedural chunks
  // that don't share vocabulary with the user's question (e.g. a query about
  // "Mental Purification" cannot retrieve the 1925 Summerschool "20 Purification
  // Breaths" transcript, which never uses the word "mental" — but it matches
  // an expansion like "Purification Breaths instructions").
  let corpusMatches = [];
  if (retrievalQuery && namespace !== 'none') {
    // Start with the original query. Ask Gemini Flash-Lite for up to 3 expansions.
    const queries = [retrievalQuery];
    try {
      const expandPrompt = `You are a search-query expander for a retrieval system over Hazrat Inayat Khan's Sufi corpus (Complete Works, Ruhaniat papers, Samuel Lewis commentaries, Pir Vilayat teachings, class transcripts).

Given a student's question, produce 2-3 alternative search queries that would retrieve DIFFERENT relevant passages the original query might miss. Focus on:
- Specific practice names the question implies but doesn't state (e.g. Nayaz, Purification Breaths, Ya Latif, Wazifa, Fikr, Qasab, Darood, Zikr, Dharana, Sarmad, Mansur)
- Procedural vocabulary ("instructions", "step by step", "rhythm", "inhale exhale", "nose mouth")
- Synonyms and lineage-specific terms (e.g. "mureed", "murshid", "samadhi", "nafs", "samskara")
- Related concepts that might sit in a different chapter

Output ONLY a JSON array of strings. No prose, no preamble. 2-3 items. Keep each under 12 words.

Student question: ${retrievalQuery}

JSON array:`;
      const expandResp = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=${env.GEMINI_API_KEY}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [{ role: 'user', parts: [{ text: expandPrompt }] }],
            generationConfig: { temperature: 0.3, maxOutputTokens: 256 },
          }),
        }
      );
      if (expandResp.ok) {
        const ed = await expandResp.json();
        let raw = ed.candidates?.[0]?.content?.parts?.[0]?.text || '';
        // Strip code fences if present
        raw = raw.replace(/```(?:json)?\s*/g, '').replace(/```\s*$/g, '').trim();
        const match = raw.match(/\[[\s\S]*\]/);
        if (match) {
          try {
            const arr = JSON.parse(match[0]);
            if (Array.isArray(arr)) {
              for (const q of arr) {
                if (typeof q === 'string' && q.trim() && queries.length < 4) {
                  queries.push(q.trim());
                }
              }
            }
          } catch (_) { /* ignore parse error — fall back to single query */ }
        }
      }
    } catch (_) { /* ignore expansion failure — fall back to single query */ }

    // Batch-embed all queries in one Workers AI call (BGE supports batch input)
    let allVecs = [];
    try {
      const embed = await env.AI.run('@cf/baai/bge-base-en-v1.5', { text: queries });
      allVecs = embed.data || embed.embeddings || [];
    } catch (_) { allVecs = []; }

    // Run parallel Vectorize queries (each topK=10) and merge by chunk_id,
    // keeping the best score per chunk.
    const filter = buildFilter();
    const byId = new Map();
    await Promise.all(allVecs.map(async (qVec, idx) => {
      if (!qVec) return;
      const searchOpts = { topK: 10, returnMetadata: 'all', returnValues: false };
      if (filter) searchOpts.filter = filter;
      try {
        const res = await env.VECTORIZE.query(qVec, searchOpts);
        for (const m of res.matches || []) {
          const prev = byId.get(m.id);
          // A boost for the original query (idx=0) since it's the most targeted;
          // expansions get their raw score.
          const score = idx === 0 ? m.score * 1.05 : m.score;
          if (!prev || score > prev.score) {
            byId.set(m.id, { score, ...m.metadata });
          }
        }
      } catch (_) { /* swallow per-query failure */ }
    }));

    corpusMatches = Array.from(byId.values())
      .sort((a, b) => b.score - a.score)
      .slice(0, 18);
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

  // The companion now draws exclusively from the vector Knowledge Library.
  // Any handbookContext / kabirContext / teachingContext / glossaryContext in
  // the request body is ignored — those sources are reserved for the user's
  // private reading experience and must never leak into AI answers.

  const systemPrompt = `You are a study companion for a Suluk Academy practitioner. Your only knowledge source is the Knowledge Library passages provided below — drawn from the Complete Works of Hazrat Inayat Khan, the Ruhaniat papers of Murshid Samuel Lewis and HIK's esoteric writings, Pir Vilayat's teachings, and related Sufi literature.

HOW TO ANSWER — MATCH THE FORMAT TO THE QUESTION:

The user is a serious practitioner. Answer the question they actually asked, in the format that best serves it. There are two modes:

MODE A — PRACTICE INSTRUCTIONS (when the user asks "how do I practice X", "step by step", "instructions for Y", "what is the procedure", "full instructions"):
- If the passages contain explicit instructions, transmit them DIRECTLY as a numbered list or clearly labeled steps. Verbatim where possible. This is the primary content, not a side note.
- FORMATTING — EACH STEP ON ITS OWN LINE. Render numbered lists as proper markdown: each item begins with "1. ", "2. ", etc. on a new line with a blank line between items (or at least a newline). Never concatenate multiple numbered items into a single paragraph like "1. Do this. 2. Do that. 3. Then this." — that is unreadable. One step = one line. If a step has a sub-explanation, keep it on the same line as the step, or indent on the next line.
- Use "## Section Header" markdown for multi-part practices. Use "- " bulleted sub-points under a step when the source lists things the practitioner should notice (e.g. the five elements, the four nose/mouth combinations).
- Include concrete specifics: breath direction (nose/mouth), breath count, thought-forms to hold, posture, time of day, prayers recited, the exact wording of any prayer given.
- Preserve structural details exactly: if the source says "20 breaths in four sets of 5" or "inhaling — exhaling" pairs, reproduce that structure.
- When the question is broad ("full instructions on Mental Purification") but the corpus contains a specific canonical procedure (like the 1925 Summerschool Purification Breaths with nose/mouth pairs and the Nayaz prayer breath-by-breath), include that procedure as a dedicated section with all its specifics — do not summarize it away.
- A brief framing sentence at the top and a short closing at the bottom are fine, but the STEPS are the answer. Do not bury them.
- The caveat about a teacher's guidance belongs at the end, in one sentence — not as a reason to withhold the instructions. HIK and his successors DID write down these practices; the corpus contains them; transmit them.

MODE B — CONCEPTUAL / CONTEMPLATIVE QUESTIONS (when the user asks "what is X", "why does the teaching say Y", "explain the significance of Z"):
- Flowing reverent prose. 4-8 paragraphs. Cross-passage synthesis.
- No bulleted lists unless the structure genuinely calls for one.

When in doubt about which mode the question calls for, check: does the user want to DO something, or UNDERSTAND something? "How" and "steps" and "instructions" → Mode A. "What" and "why" → Mode B.

UNIVERSAL STYLE RULES:
- Open with "Dear Mureed," when the question is a genuine spiritual inquiry. Skip the salutation for short factual follow-ups ("what does X mean?", "who said this?") and for direct procedural requests where the salutation would delay the steps.
- Close with a brief invocation or blessing — but VARY the closing. Never repeat the same closing across a conversation. Rotate through forms such as:
  * "May your heart find its tuning in the One."
  * "May the breath carry you gently toward the light."
  * "May this contemplation bear fruit in your practice."
  * "Salaam, and may the Beloved illumine your path."
  * "Toward the One — in whose presence all questions dissolve."
  * "May you walk in remembrance."
  * "Ya Latif. May subtlety be your companion."
  * Or a closing that rises naturally from the content of the answer itself.
- Direct quotations from HIK, Murshid Samuel Lewis, or Pir Vilayat are welcome — place them in double quotes.
- Do NOT print inline citations. No bracket tags like [L1]. No parenthetical book names, no page numbers, no " — Source X — Chapter Y" attributions mid-sentence. You may name the teacher when ascribing a direct quotation, but never the book title or page.
- Do NOT end the answer with a "Sources" list or any reference block.

IMPORTANT RULES:
1. Ground every answer in the provided passages. If the passages do not address the question, say so plainly — do not invent.
2. If the passages DO contain the requested practice or instruction, transmit it. Do not redirect the student to "find a teacher" as a way of withholding content that is present in the corpus. The corpus itself is a transmission.
3. Be respectful and reverent toward the teachings and the Inayatiyya lineage.
4. Give a thorough, precise answer. Do not truncate; develop the teaching until the question is fully answered.` + corpusExcerpts;

  const geminiMessages = messages.map(m => ({
    role: m.role === 'assistant' ? 'model' : 'user',
    parts: [{ text: m.content }],
  }));

  const geminiBody = {
    system_instruction: { parts: [{ text: systemPrompt }] },
    contents: geminiMessages,
    generationConfig: { temperature: 0.6, maxOutputTokens: 8192 },
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
