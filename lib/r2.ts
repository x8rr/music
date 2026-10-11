// Cloudflare R2: the permanent store behind the on-disk music caches (album
// art, yt-dlp audio). The disk keeps the hot copies and the sweep holds it
// small; R2 keeps everything, so an evicted cover or track comes back from
// here instead of from a provider that rate-limits us or a fresh yt-dlp run.
//
// There is no built-in bucket or endpoint: whoever runs this sets R2_BASE to
// their own S3 API endpoint (https://<account id>.r2.cloudflarestorage.com)
// along with the keys and bucket. With any of them missing `r2` is null and
// everything runs on local disk only.

import { S3Client } from "bun";

function client(): S3Client | null {
  const { R2_BASE, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET } = process.env;
  const endpoint = (R2_BASE || "").trim().replace(/\/+$/, "");
  if (!endpoint || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) return null;
  if (!/^https?:\/\//i.test(endpoint)) {
    console.warn("[r2] R2_BASE must be an http(s) URL; running on local disk only");
    return null;
  }
  return new S3Client({
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY,
    bucket: R2_BUCKET,
    endpoint,
  });
}

export const r2 = client();

const HEADERS_TIMEOUT_MS = 10_000;

/**
 * The object as a Response, or null when it is missing or R2 can't answer:
 * callers fall back to the origin rather than failing a cover or a play.
 * Fetched through a presigned URL so the one GET also carries the stored
 * content type. The timeout covers the headers only; a slow body is left to
 * the caller's own reader.
 */
export async function r2Get(key: string): Promise<Response | null> {
  if (!r2) return null;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), HEADERS_TIMEOUT_MS);
  try {
    const res = await fetch(r2.presign(key, { expiresIn: 60 }), { signal: ctrl.signal });
    if (res.ok && res.body) return res;
    await res.body?.cancel().catch(() => {});
    if (res.status !== 404) console.warn(`[r2] get ${key}: ${res.status}`);
  } catch (err) {
    console.warn(`[r2] get ${key}:`, err instanceof Error ? err.message : err);
  } finally {
    clearTimeout(timer);
  }
  return null;
}

/** Stores a copy. Failure is logged, not thrown: everything kept here can be
 * fetched again from its origin, so a lost write costs one refetch later. */
export async function r2Put(key: string, body: Buffer | Blob, type: string): Promise<void> {
  if (!r2) return;
  try {
    await r2.write(key, body, { type });
  } catch (err) {
    console.warn(`[r2] put ${key}:`, err instanceof Error ? err.message : err);
  }
}

/**
 * Uploads a file from disk; throws on failure. Streamed through a presigned
 * PUT because both obvious routes hold the file in memory: under Linux
 * `r2.write(key, Bun.file(...))` never let go of it (~3 MB a track, untouched
 * by a forced GC), which OOM-killed the backfill at 7.9 GB, and fetch with a
 * BunFile body reads the whole file first. Measured on prod, five ~180 MB
 * tracks at once peak at 86 MB this way. R2 takes up to 5 GB in one PUT.
 */
export async function r2PutFile(key: string, file: string, type: string): Promise<void> {
  if (!r2) return;
  const body = Bun.file(file);
  const res = await fetch(r2.presign(key, { method: "PUT", expiresIn: 3600, type }), {
    method: "PUT",
    body: body.stream(),
    // A stream carries no length of its own; the signed PUT needs one.
    headers: { "content-type": type, "content-length": String(body.size) },
    duplex: "half",
  } as RequestInit);
  await res.body?.cancel().catch(() => {});
  if (!res.ok) throw new Error(`R2 answered ${res.status}`);
}
