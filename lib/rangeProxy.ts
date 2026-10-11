/**
 * Bounded-memory proxy for large upstream bodies (audio streams, ROMs).
 *
 * Bun's fetch() has no backpressure: once a response starts, the whole body is
 * pulled into native memory as fast as the upstream sends it, whether or not
 * anyone reads it. Reading slowly does not slow the socket. A browser playing
 * audio reads a few hundred KB, then stalls until its buffer drains, so a
 * plain pipe of a 40MB track keeps ~40MB resident per listener for the whole
 * song — and Chrome holds paused media connections open, so it stays resident
 * after the listener hits pause too. That is what took the process to 8GB.
 *
 * The fix is to never ask the upstream for more than one chunk at a time: the
 * body is fetched as a sequence of small Range requests, each pumped to the
 * client with proper write/drain handling before the next one is issued. The
 * memory ceiling per stream is then one chunk, not one file, and a paused
 * player costs nothing because nothing is fetched until it reads again.
 *
 * Upstreams that ignore Range (a 200 to a ranged request) fall back to a
 * single pumped pass over the full body, so nothing that used to play stops
 * playing; it just loses the bound.
 */
import { once } from "node:events";
import type { OutgoingHttpHeaders, ServerResponse } from "node:http";
import type { FastifyReply, FastifyRequest } from "fastify";

// Per active listener this is the resident upstream buffer, so it is also the
// per-listener memory cost of proxied playback: 1 MiB × 1000 listeners is
// 1 GB. Halved from 2 MiB; the extra upstream requests are cheap.
export const DEFAULT_CHUNK_BYTES = 1 * 1024 * 1024;
/** Covers connect + the full chunk for one upstream request. */
const UPSTREAM_CHUNK_TIMEOUT_MS = 30_000;
/** A client that has not drained its socket in this long is gone, whatever the
 * socket says. Bun's ServerResponse does not emit 'close' on a peer abort, so
 * an await on 'drain' would otherwise hang forever, pinning the chunk. */
const DRAIN_TIMEOUT_MS = 60_000;

export interface ClientRange {
  start: number;
  /** null: open-ended ("bytes=100-"). */
  end: number | null;
}

/** `bytes=a-b` / `bytes=a-`. Anything else (suffix ranges, multiple ranges)
 * returns "unsupported"; missing returns null. */
export function parseRangeHeader(
  header: string | undefined,
): ClientRange | null | "unsupported" {
  if (header === undefined) return null;
  const m = /^bytes=(\d+)-(\d*)$/.exec(header.trim());
  if (!m) return "unsupported";
  const start = Number(m[1]);
  const end = m[2] === "" ? null : Number(m[2]);
  if (!Number.isSafeInteger(start) || (end !== null && (!Number.isSafeInteger(end) || end < start)))
    return "unsupported";
  return { start, end };
}

export interface ContentRange {
  start: number;
  end: number;
  /** null when the upstream answered `bytes a-b/*`. */
  total: number | null;
}

export function parseContentRange(header: string | null): ContentRange | null {
  if (!header) return null;
  const m = /^bytes (\d+)-(\d+)\/(\d+|\*)$/.exec(header.trim());
  if (!m) return null;
  const start = Number(m[1]);
  const end = Number(m[2]);
  if (end < start) return null;
  return { start, end, total: m[3] === "*" ? null : Number(m[3]) };
}

export interface RangeProxyOptions {
  /** Sent on every upstream request (User-Agent, auth). Range is added here. */
  headers?: Record<string, string>;
  /** Extra headers for the client response (cache-control, CORS). */
  responseHeaders?: OutgoingHttpHeaders;
  /** Used when the upstream sends no content-type. */
  fallbackContentType?: string;
  chunkBytes?: number;
  /** Upstream statuses to relay as-is instead of folding into a 502. A game
   * probing for an optional asset needs to see a real 404. */
  passStatuses?: number[];
  /** Aborts every upstream request the moment the client goes away. */
  signal?: AbortSignal;
}

export type RangeProxyOutcome =
  | "complete"
  | "client-gone"
  | "upstream-failed"
  | "upstream-error";

interface ClientLink {
  res: ServerResponse;
  isClosed: () => boolean;
  /** Resolves when the client disconnects; races the drain wait. */
  closed: Promise<void>;
}

function link(req: FastifyRequest, reply: FastifyReply): ClientLink {
  const res = reply.raw;
  let gone = false;
  const closed = new Promise<void>((resolve) => {
    // 'close' on the request is the one event Bun does fire on a peer abort
    // (the response never emits it). It also fires after a normal finish,
    // which is harmless: by then nothing is waiting on it.
    req.raw.on("close", () => {
      gone = true;
      resolve();
    });
  });
  return {
    res,
    closed,
    isClosed: () =>
      gone || res.destroyed === true || res.socket?.destroyed === true || res.writableEnded,
  };
}

/** Writes one body to the client with backpressure. Resolves true when the
 * body was fully delivered, false when the client went away first. */
async function pump(
  body: ReadableStream<Uint8Array>,
  client: ClientLink,
): Promise<boolean> {
  const reader = body.getReader();
  try {
    for (;;) {
      if (client.isClosed()) {
        await reader.cancel().catch(() => {});
        return false;
      }
      const { done, value } = await reader.read();
      if (done) return true;
      if (!value || value.byteLength === 0) continue;
      let ok: boolean;
      try {
        ok = client.res.write(value);
      } catch {
        await reader.cancel().catch(() => {});
        return false;
      }
      if (ok) continue;
      let drainTimer: ReturnType<typeof setTimeout> | undefined;
      const drained = await Promise.race([
        once(client.res, "drain").then(() => true),
        client.closed.then(() => false),
        new Promise<boolean>((resolve) => {
          drainTimer = setTimeout(() => resolve(false), DRAIN_TIMEOUT_MS);
        }),
      ]).finally(() => clearTimeout(drainTimer));
      if (!drained) {
        await reader.cancel().catch(() => {});
        return false;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

async function fetchChunk(
  url: string,
  headers: Record<string, string>,
  range: string,
  outer: AbortSignal | undefined,
): Promise<{ res: Response; release: () => void }> {
  const ctrl = new AbortController();
  const onOuter = () => ctrl.abort();
  outer?.addEventListener("abort", onOuter, { once: true });
  if (outer?.aborted) ctrl.abort();
  const timer = setTimeout(() => ctrl.abort(), UPSTREAM_CHUNK_TIMEOUT_MS);
  const release = () => {
    clearTimeout(timer);
    outer?.removeEventListener("abort", onOuter);
  };
  try {
    const res = await fetch(url, {
      headers: { ...headers, range },
      signal: ctrl.signal,
    });
    return { res, release };
  } catch (err) {
    release();
    throw err;
  }
}

/**
 * Serves `upstreamUrl` to the client in bounded chunks. Handles the reply
 * completely (status, headers, body); the caller must not touch it afterwards.
 * Any failure before the first byte is a 502 with `{ error }`; a failure after
 * that drops the connection so the player sees a truncated transfer rather
 * than a silent stall.
 */
export async function proxyUpstreamRanged(
  req: FastifyRequest,
  reply: FastifyReply,
  upstreamUrl: string,
  opts: RangeProxyOptions = {},
): Promise<RangeProxyOutcome> {
  const chunkBytes = Math.max(64 * 1024, opts.chunkBytes ?? DEFAULT_CHUNK_BYTES);
  const headers = opts.headers ?? {};
  const wanted = parseRangeHeader(req.headers.range);

  // A range shape we cannot slice (suffix/multi) is passed through whole; the
  // players we serve never send one, so this is a correctness fallback only.
  const first = wanted === "unsupported"
    ? { start: 0, end: null as number | null, passthrough: req.headers.range as string }
    : { start: wanted?.start ?? 0, end: wanted?.end ?? null, passthrough: null };

  const rangeFor = (from: number, to: number | null) =>
    `bytes=${from}-${to === null ? "" : to}`;
  const chunkEnd = (from: number) =>
    first.end === null ? from + chunkBytes - 1 : Math.min(from + chunkBytes - 1, first.end);

  let head: { res: Response; release: () => void };
  try {
    head = await fetchChunk(
      upstreamUrl,
      headers,
      first.passthrough ?? rangeFor(first.start, chunkEnd(first.start)),
      opts.signal,
    );
  } catch (e) {
    reply.code(502).send({
      error: "Upstream fetch failed",
      detail: e instanceof Error ? e.message : String(e),
    });
    return "upstream-failed";
  }

  const up = head.res;
  const contentType =
    up.headers.get("content-type") || opts.fallbackContentType || "application/octet-stream";

  if (up.status === 416) {
    head.release();
    up.body?.cancel().catch(() => {});
    const cr = up.headers.get("content-range");
    if (cr) reply.header("content-range", cr);
    reply.code(416).send();
    return "upstream-error";
  }
  if (!up.ok || !up.body) {
    head.release();
    up.body?.cancel().catch(() => {});
    if (opts.passStatuses?.includes(up.status)) {
      reply.code(up.status).send({ error: `Upstream returned ${up.status}` });
      return "upstream-error";
    }
    reply.code(502).send({ error: `Upstream returned ${up.status}` });
    return "upstream-error";
  }

  const client = link(req, reply);
  const base: OutgoingHttpHeaders = {
    ...(reply.getHeaders() as OutgoingHttpHeaders),
    ...(opts.responseHeaders ?? {}),
    "content-type": contentType,
    "accept-ranges": up.headers.get("accept-ranges") || "bytes",
  };

  // Upstream ignored the Range header: one body, one pass, no bound. Still
  // pumped rather than piped so a stalled client stops the read.
  const cr = up.status === 206 ? parseContentRange(up.headers.get("content-range")) : null;
  if (up.status !== 206 || !cr || first.passthrough !== null) {
    reply.hijack();
    const len = up.headers.get("content-length");
    const h: OutgoingHttpHeaders = { ...base };
    if (up.status === 206) {
      const ucr = up.headers.get("content-range");
      if (ucr) h["content-range"] = ucr;
    }
    if (len) h["content-length"] = len;
    client.res.on("error", () => {});
    client.res.writeHead(up.status, h);
    let delivered = false;
    try {
      delivered = await pump(up.body, client);
    } finally {
      head.release();
    }
    finish(client, delivered);
    return delivered ? "complete" : "client-gone";
  }

  // Ranged path. The client gets one response spanning everything it asked
  // for; the upstream is asked for it one chunk at a time.
  const total = cr.total;
  const lastByte =
    first.end !== null
      ? total !== null ? Math.min(first.end, total - 1) : first.end
      : total !== null ? total - 1 : null;

  reply.hijack();
  client.res.on("error", () => {});
  const h: OutgoingHttpHeaders = { ...base };
  if (wanted === null) {
    if (total !== null) h["content-length"] = String(total);
    client.res.writeHead(200, h);
  } else {
    h["content-range"] = `bytes ${first.start}-${lastByte ?? ""}/${total ?? "*"}`;
    if (lastByte !== null) h["content-length"] = String(lastByte - first.start + 1);
    client.res.writeHead(206, h);
  }

  let pos = first.start;
  let current: { res: Response; release: () => void } | null = head;
  let expected: ContentRange | null = cr;
  try {
    for (;;) {
      if (!current || !expected || !current.res.body) break;
      const delivered = await pump(current.res.body, client);
      current.release();
      current = null;
      if (!delivered) {
        finish(client, false);
        return "client-gone";
      }
      pos = expected.end + 1;
      const done =
        (lastByte !== null && pos > lastByte) ||
        (expected.total !== null && pos >= expected.total);
      if (done) {
        finish(client, true);
        return "complete";
      }
      if (client.isClosed()) {
        finish(client, false);
        return "client-gone";
      }

      try {
        current = await fetchChunk(
          upstreamUrl,
          headers,
          rangeFor(pos, lastByte === null ? pos + chunkBytes - 1 : Math.min(pos + chunkBytes - 1, lastByte)),
          opts.signal,
        );
      } catch {
        current = null;
        break;
      }
      const r = current.res;
      if (r.status === 416 && lastByte === null) {
        // Open-ended request with an unknown total: this is the end.
        current.release();
        r.body?.cancel().catch(() => {});
        current = null;
        finish(client, true);
        return "complete";
      }
      expected = r.status === 206 ? parseContentRange(r.headers.get("content-range")) : null;
      if (!r.ok || !r.body || !expected || expected.start !== pos) {
        current.release();
        r.body?.cancel().catch(() => {});
        current = null;
        break;
      }
    }
  } finally {
    if (current) {
      current.release();
      current.res.body?.cancel().catch(() => {});
    }
  }

  // Mid-stream upstream failure: the headers promised more bytes than were
  // sent, so tear the connection down instead of ending it cleanly.
  try {
    client.res.destroy();
  } catch {
    // Already gone.
  }
  return "upstream-error";
}

function finish(client: ClientLink, delivered: boolean) {
  try {
    if (delivered) client.res.end();
    else client.res.destroy();
  } catch {
    // The peer closed first; nothing left to release.
  }
}
