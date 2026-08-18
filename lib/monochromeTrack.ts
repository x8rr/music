// Monochrome's own playback resolver ("Rythm playback resolver", per its
// OpenAPI) at track-api.monochrome.tf.
//
// This exists because the shared-mirror ecosystem cherri used for lossless
// collapsed: the Tidal stream mirrors now serve 30-second previews (their
// sessions aren't entitled), every Qobuz mirror is gone or has expired Qobuz
// credentials, and the Deezer bridge reports all its ARLs banned. Monochrome
// responded by running their own resolver, and this is the client for it.
//
// It resolves by name rather than by ID — song_name + artist, optionally
// narrowed by isrc and duration — and hands back a direct FLAC URL. That maps
// cleanly onto the StreamMeta the music route already carries around.
//
// The catch is Cloudflare Turnstile: /playback needs a bearer token that can
// only be minted by exchanging a Turnstile solve, and Turnstile deliberately
// requires a real browser. So a human visits /api/music/monochrome/auth once,
// and the resulting session is cached here and reused for everything until it
// expires. This mirrors what server/routes/amazon.ts already does — same
// Turnstile site key, in fact.

import { readFileSync, writeFileSync, mkdirSync } from "fs";
import path from "path";

const BASE = (
  process.env.MONOCHROME_TRACK_API || "https://track-api.monochrome.tf"
).replace(/\/+$/, "");

// Published by the API's own /config; kept here only as a fallback for when
// that call fails, so the auth page can still render something usable.
const FALLBACK_SITE_KEY =
  process.env.MONOCHROME_TURNSTILE_SITE_KEY || "0x4AAAAAADgxqF6QVMm0GLHH";

// A bypass token from monochrome, if they issue one. Their API takes it as a
// query param on /playback and makes the Authorization header optional, so it
// replaces the whole Turnstile dance.
//
// This matters beyond convenience: Turnstile sitekeys are locked to their
// owner's hostnames, and the key above is monochrome's. Any origin that is not
// on their allowlist gets error 110200 before a challenge is even issued —
// which makes the browser flow unworkable for bring-your-own-domain
// deployments. A bypass token is server-side only, so it works from every
// domain at once and never expires mid-session.
const BYPASS_TOKEN = process.env.MONOCHROME_BYPASS_TOKEN || "";

const SESSION_FILE = path.join(
  process.cwd(),
  "data",
  "monochrome-session.json",
);

interface Session {
  value: string;
  exp: number;
}

let session: Session | null = (() => {
  try {
    const s = JSON.parse(readFileSync(SESSION_FILE, "utf-8"));
    if (s?.value && s?.exp && Date.now() < s.exp) return s;
  } catch {
    // No session on disk, or it's stale — the operator re-solves.
  }
  return null;
})();

function persistSession() {
  try {
    mkdirSync(path.dirname(SESSION_FILE), { recursive: true });
    writeFileSync(SESSION_FILE, JSON.stringify(session));
  } catch {
    // A read-only data dir just means the session won't survive a restart.
  }
}

/** Set when the API answers 429, so we stop hammering it until it says we may
 * resume. Monochrome's own client does the same. */
let rateLimitedUntil = 0;

export function isAuthed(): boolean {
  // A bypass token needs no session at all.
  return !!BYPASS_TOKEN || !!(session && Date.now() < session.exp);
}

export function authState(): {
  authenticated: boolean;
  bypass: boolean;
  expiresAt: number | null;
  rateLimitedUntil: number | null;
  base: string;
} {
  return {
    authenticated: isAuthed(),
    bypass: !!BYPASS_TOKEN,
    expiresAt: session?.exp ?? null,
    rateLimitedUntil: rateLimitedUntil > Date.now() ? rateLimitedUntil : null,
    base: BASE,
  };
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit = {},
  ms = 20_000,
): Promise<Response> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), ms);
  try {
    return await fetch(url, { ...init, signal: ctrl.signal });
  } catch (e) {
    if (e instanceof Error && e.name === "AbortError")
      throw new Error(`monochrome request timed out after ${ms}ms (${url})`);
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

interface MonochromeConfig {
  turnstile_enabled: boolean;
  turnstile_site_key: string;
  turnstile_action: string;
  session_ttl_seconds: number;
}

let configCache: { value: MonochromeConfig; ts: number } | null = null;
const CONFIG_TTL_MS = 10 * 60 * 1000;

/** The site key and action come from the API rather than being hardcoded, so a
 * rotation upstream doesn't silently break the auth page. */
export async function getConfig(): Promise<MonochromeConfig> {
  if (configCache && Date.now() - configCache.ts < CONFIG_TTL_MS) {
    return configCache.value;
  }
  try {
    const res = await fetchWithTimeout(`${BASE}/config`, {}, 10_000);
    if (!res.ok) throw new Error(`config HTTP ${res.status}`);
    const value = (await res.json()) as MonochromeConfig;
    if (!value?.turnstile_site_key) throw new Error("config had no site key");
    configCache = { value, ts: Date.now() };
    return value;
  } catch {
    return {
      turnstile_enabled: true,
      turnstile_site_key: FALLBACK_SITE_KEY,
      turnstile_action: "auth",
      session_ttl_seconds: 3600,
    };
  }
}

export async function exchangeTurnstile(token: string): Promise<number> {
  const res = await fetchWithTimeout(`${BASE}/auth/turnstile`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ turnstile_token: token }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`turnstile exchange failed: ${res.status} ${body.slice(0, 200)}`);
  }
  const data = (await res.json()) as {
    access_token?: string;
    expires_in?: number;
  };
  if (!data.access_token) throw new Error("no access_token in response");

  // Expire a minute early so a resolve never races the boundary.
  const ttl = (data.expires_in ?? 3600) * 1000;
  session = { value: data.access_token, exp: Date.now() + ttl - 60_000 };
  persistSession();
  rateLimitedUntil = 0;
  return session.exp;
}

export function clearSession() {
  session = null;
  persistSession();
}

export interface MonochromeMeta {
  title?: string;
  artist?: string;
  isrc?: string;
  duration?: number;
}

export interface MonochromeResult {
  url: string;
  trackId: string | null;
  recordingId: string | null;
  title: string | null;
  durationSeconds: number | null;
}

export async function resolveMonochromeStream(
  meta: MonochromeMeta,
): Promise<MonochromeResult> {
  if (!meta.title || !meta.artist) {
    throw new Error("monochrome needs a title and artist");
  }
  if (Date.now() < rateLimitedUntil) {
    throw new Error("monochrome rate limited; backing off");
  }
  if (!BYPASS_TOKEN && !isAuthed()) {
    throw new Error("monochrome not authenticated — solve Turnstile first");
  }

  const body: Record<string, unknown> = {
    song_name: meta.title,
    artist: meta.artist,
  };
  if (meta.isrc) body.isrc = meta.isrc;
  if (meta.duration) body.duration = Math.round(meta.duration);

  // bypass_token rides in the query string; the Authorization header is
  // optional when it is present.
  const url = BYPASS_TOKEN
    ? `${BASE}/playback?bypass_token=${encodeURIComponent(BYPASS_TOKEN)}`
    : `${BASE}/playback`;
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (session) headers.Authorization = `Bearer ${session.value}`;

  const res = await fetchWithTimeout(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  if (res.status === 401) {
    // The session died before its stated expiry. Drop it so the next request
    // reports "not authenticated" instead of retrying a dead token. With a
    // bypass token this means the token itself was rejected.
    if (BYPASS_TOKEN) throw new Error("monochrome rejected the bypass token");
    clearSession();
    throw new Error("monochrome session rejected — re-authenticate");
  }
  if (res.status === 429) {
    const retryAfter = res.headers.get("Retry-After");
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds) && seconds > 0) {
      rateLimitedUntil = Date.now() + seconds * 1000;
    } else {
      const at = Date.parse(retryAfter || "");
      rateLimitedUntil = Number.isFinite(at) ? at : Date.now() + 30_000;
    }
    throw new Error("monochrome rate limited (429)");
  }
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`monochrome playback failed: ${res.status} ${text.slice(0, 200)}`);
  }

  const data = (await res.json()) as {
    url?: string;
    track_id?: string;
    recording_id?: string;
    title?: string;
    duration_seconds?: number;
  };
  if (!data.url) throw new Error("monochrome returned no stream URL");

  return {
    url: data.url,
    trackId: data.track_id ?? null,
    recordingId: data.recording_id ?? null,
    title: data.title ?? null,
    durationSeconds: data.duration_seconds ?? null,
  };
}

export const AUTH_PAGE_HTML = (siteKey: string) => `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monochrome playback auth</title>
<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>
<style>body{font-family:system-ui,sans-serif;background:#0d0d0f;color:#e8e8ea;display:grid;place-items:center;height:100vh;margin:0}main{text-align:center;max-width:32rem}#s{margin-top:1rem;opacity:.8;font-size:14px}</style>
</head><body><main>
<h2>Authenticate Monochrome playback</h2>
<div class="cf-turnstile" data-sitekey="${siteKey}" data-action="auth" data-callback="onSolve"></div>
<p id="s">Solve the challenge to enable lossless streaming. The session lasts about an hour and is shared by the server.</p>
</main><script>
function onSolve(token){
  var s=document.getElementById('s');s.textContent='Exchanging\\u2026';
  fetch('/api/music/monochrome/turnstile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({turnstile_token:token})})
    .then(function(r){return r.json()}).then(function(d){
      s.textContent=d.ok?('\\u2713 Authenticated until '+new Date(d.expiresAt).toLocaleTimeString()):('\\u2717 '+(d.error||'failed'));
    }).catch(function(e){s.textContent='\\u2717 '+e.message});
}
</script></body></html>`;
