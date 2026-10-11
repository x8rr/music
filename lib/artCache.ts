// Disk-cached proxy for album art.
//
// Artwork comes from whichever provider answered the search — Tidal, YouTube
// Music, SoundCloud — and the browse grid asks for dozens of covers at once.
// Google rate-limits that burst: yt3.googleusercontent.com starts answering
// 429 with an HTML error page instead of a JPEG, so <img> tags break and the
// artwork Chrome fetches for the OS media controls fails too, leaving a
// generic icon. Fetching once from the server and serving from disk turns N
// client requests per cover into one upstream request, ever.

import { createReadStream, promises as fsp } from "node:fs";
import type { ReadStream } from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { readBodyAtMost } from "./readBodyAtMost";
import { touchIfStale } from "./cacheSweep";
import { r2Get, r2Put } from "./r2";

// Read per call rather than once at import: the module is shared across test
// files in one process, and a test that points ART_CACHE_DIR at a temp dir
// must not depend on being the first importer.
function cacheDir(): string {
  return process.env.ART_CACHE_DIR || path.join(process.cwd(), "data", "art-cache");
}

const FETCH_TIMEOUT_MS = Number(process.env.ART_FETCH_TIMEOUT_MS) || 8000;

// Covers are small; anything larger is not album art and we refuse to cache it.
const MAX_BYTES = Number(process.env.ART_MAX_BYTES) || 5 * 1024 * 1024;

/**
 * Hosts this proxy will fetch from.
 *
 * This is the whole security boundary: without it the endpoint is an open
 * proxy that will fetch any URL a caller names, including link-local metadata
 * services and anything else reachable from the server. Suffix entries are
 * matched on a dot boundary so "evilgoogleusercontent.com" cannot pass as
 * ".googleusercontent.com".
 */
const ALLOWED_HOSTS: readonly string[] = [
  "resources.tidal.com",
  "i.ytimg.com",
  "static.qobuz.com",
  "i1.sndcdn.com",
  "i2.sndcdn.com",
  "i3.sndcdn.com",
  "i4.sndcdn.com",
];

const ALLOWED_SUFFIXES: readonly string[] = [
  ".googleusercontent.com",
  ".sndcdn.com",
  ".scdn.co",
  ".tidal.com",
];

export function isAllowedArtUrl(raw: string): boolean {
  let u: URL;
  try {
    u = new URL(raw);
  } catch {
    return false;
  }
  if (u.protocol !== "https:") return false;
  const host = u.hostname.toLowerCase();
  if (ALLOWED_HOSTS.includes(host)) return true;
  return ALLOWED_SUFFIXES.some((s) => host.endsWith(s));
}

function cacheKey(url: string): string {
  return crypto.createHash("sha256").update(url).digest("hex").slice(0, 32);
}

// Two-character shards: one flat directory held a million entries, which
// made every readdir (the sweep, a shell listing, a backup) a multi-minute
// job. Entries written before sharding are found on the flat path and moved
// into their shard on first hit, so nothing walks the old tree at once.
function shardedPath(key: string): string {
  return path.join(cacheDir(), key.slice(0, 2), key);
}
function flatPath(key: string): string {
  return path.join(cacheDir(), key);
}

export interface CachedArtFile {
  file: string;
  size: number;
  contentType: string;
}

/** Extension-free: the content type is stored beside the bytes in a sidecar so
 * a cache hit doesn't have to guess it from the URL. */
async function readCached(key: string): Promise<CachedArtFile | null> {
  const sharded = shardedPath(key);
  const hit = await statPair(sharded);
  if (hit) return hit;
  const flat = flatPath(key);
  const legacy = await statPair(flat);
  if (!legacy) return null;
  // Move the pair into its shard; a concurrent reader that loses the race
  // still finds it at one of the two paths.
  try {
    await fsp.mkdir(path.dirname(sharded), { recursive: true });
    await fsp.rename(`${flat}.type`, `${sharded}.type`);
    await fsp.rename(flat, sharded);
    return { ...legacy, file: sharded };
  } catch {
    return legacy;
  }
}

async function statPair(file: string): Promise<CachedArtFile | null> {
  try {
    const [st, contentType] = await Promise.all([
      fsp.stat(file),
      fsp.readFile(`${file}.type`, "utf8"),
    ]);
    if (!st.isFile() || st.size === 0) return null;
    touchIfStale(file, st);
    return { file, size: st.size, contentType: contentType.trim() || "image/jpeg" };
  } catch {
    return null;
  }
}

async function writeCached(key: string, body: Buffer, contentType: string): Promise<CachedArtFile> {
  const final = shardedPath(key);
  await fsp.mkdir(path.dirname(final), { recursive: true });
  // Write to a temp name and rename in, so a concurrent reader never sees a
  // half-written file as a cache hit.
  const tag = crypto.randomBytes(4).toString("hex");
  const tmp = `${final}.${tag}.tmp`;
  await fsp.writeFile(tmp, body);
  await fsp.writeFile(`${tmp}.type`, contentType);
  await fsp.rename(`${tmp}.type`, `${final}.type`);
  await fsp.rename(tmp, final);
  return { file: final, size: body.byteLength, contentType };
}

// One upstream fetch per URL even when the whole grid asks at once — the
// stampede is the thing that got us rate-limited in the first place.
const inflight = new Map<string, Promise<CachedArtFile>>();

// A URL that just failed (429, HTML error page, too large) is refused for a
// while without going upstream again: the grid retries broken covers on
// every render, and each retry used to be another rate-limited fetch.
const NEGATIVE_TTL_MS = 10 * 60_000;
const NEGATIVE_MAX = 20_000;
const negative = new Map<string, { until: number; reason: string }>();

function rememberFailure(key: string, reason: string) {
  if (negative.size >= NEGATIVE_MAX) {
    const oldest = negative.keys().next().value;
    if (oldest !== undefined) negative.delete(oldest);
  }
  negative.set(key, { until: Date.now() + NEGATIVE_TTL_MS, reason });
}

export class ArtUnavailableError extends Error {
  constructor(reason: string) {
    super(reason);
    this.name = "ArtUnavailableError";
  }
}

async function fetchArt(url: string): Promise<{ body: Buffer; contentType: string }> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        // Bare fetches from a datacentre IP are what get 429'd hardest.
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        Accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
      },
    });
    if (!res.ok) {
      await res.body?.cancel().catch(() => {});
      throw new Error(`upstream ${res.status}`);
    }
    const contentType = res.headers.get("content-type") || "";
    // A 200 carrying HTML is an error page wearing a success code — exactly
    // what the rate limiter returns. Caching it would pin the breakage.
    if (!contentType.startsWith("image/")) {
      await res.body?.cancel().catch(() => {});
      throw new Error(`not an image (${contentType || "no content-type"})`);
    }
    if (!res.body) throw new Error("empty response");
    // Bounded while reading, not checked after: arrayBuffer() would have
    // pulled a multi-hundred-megabyte "image" fully into memory first.
    const buf = await readBodyAtMost(res.body, MAX_BYTES);
    if (!buf) throw new Error(`too large (> ${MAX_BYTES} bytes)`);
    if (buf.byteLength === 0) throw new Error("empty response");
    return { body: buf, contentType };
  } finally {
    clearTimeout(timer);
  }
}

/** Where every cover is kept for good; the disk holds only the popular ones.
 * An object so tests can swap in a Map. */
export const artStore = { get: r2Get, put: r2Put };

async function fromStore(key: string): Promise<CachedArtFile | null> {
  const res = await artStore.get(`art/${key}`);
  if (!res?.body) return null;
  const body = await readBodyAtMost(res.body, MAX_BYTES).catch(() => null);
  if (!body?.byteLength) return null;
  return writeCached(key, body, res.headers.get("content-type") || "image/jpeg");
}

/**
 * Cached artwork for `url` as a file on disk, fetching on a miss. Throws if
 * the upstream fails or answers with something that isn't an image; a recent
 * failure is replayed from memory as ArtUnavailableError without a fetch.
 */
export async function getArt(url: string): Promise<CachedArtFile> {
  const key = cacheKey(url);

  const hit = await readCached(key);
  if (hit) return hit;

  const bad = negative.get(key);
  if (bad) {
    if (bad.until > Date.now()) throw new ArtUnavailableError(bad.reason);
    negative.delete(key);
  }

  const existing = inflight.get(key);
  if (existing) return existing;

  const job = (async () => {
    const stored = await fromStore(key);
    if (stored) return stored;
    let art: { body: Buffer; contentType: string };
    try {
      art = await fetchArt(url);
    } catch (err) {
      rememberFailure(key, err instanceof Error ? err.message : String(err));
      throw err;
    }
    const cached = await writeCached(key, art.body, art.contentType);
    void artStore.put(`art/${key}`, art.body, art.contentType);
    return cached;
  })();

  inflight.set(key, job);
  job.catch(() => {}).finally(() => inflight.delete(key));
  return job;
}

/** A readable for a cache hit; the route pipes it rather than buffering. */
export function openArt(art: CachedArtFile): ReadStream {
  return createReadStream(art.file);
}

export function artCacheDir(): string {
  return cacheDir();
}

/** Diagnostic counts for /api/system/memory. */
export function artCacheStats() {
  return { inflight: inflight.size, negative: negative.size };
}
