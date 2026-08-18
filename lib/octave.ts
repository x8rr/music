// Octave streaming (octavestreaming.com) serves direct 320kbps MP3 URLs for
// Tidal tracks -- the same track ids the `tidal` source already searches.
// Each URL is signed with a playback token (`k`), stored by the octave web
// app in localStorage as `octave:pbtoken`. There is no public API that issues
// it, so it is minted by driving a real browser: scripts/get-pbtoken.mjs.
//
// The token is a shared server-side credential. music.ts attaches it when it
// builds an octave stream URL; it is never handed to the client. While no
// token has been fetched, the octave source just loses the stream race and
// cherri falls through to the other providers.

import { spawn } from "node:child_process";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Read per call, not at module load, so tests and operators can switch bases
// without a restart.
function octaveBase(): string {
  return (process.env.OCTAVE_API_BASE || "https://api.octavestreaming.com").replace(
    /\/+$/,
    "",
  );
}

// Operator escape hatch: skip the whole browser dance and paste a freshly
// minted token here. Never expires, since we can't see into the upstream.
const TOKEN_OVERRIDE = process.env.OCTAVE_PBTOKEN || "";

const TTL_MS = Number(process.env.OCTAVE_PBTOKEN_TTL_MS) || 12 * 60 * 60 * 1000;

// A failed run launches (then kills) a browser for nothing. Back off before
// relaunching so a dead upstream — or an absent playwright/chromium — fails
// one play and then costs nothing until the window lapses.
const RETRY_COOLDOWN_MS =
  Number(process.env.OCTAVE_PBTOKEN_RETRY_MS) || 5 * 60 * 1000;

const SCRIPT_RUN_TIMEOUT_MS = 90 * 1000;

// Search sits in front of every octave play, so it gets a tight budget: it
// answers in ~0.5s when healthy, and a slow one should lose the race rather
// than hold the play up.
const SEARCH_TIMEOUT_MS = Number(process.env.OCTAVE_SEARCH_TIMEOUT_MS) || 4000;

const TOKEN_RE = /^octk_[A-Za-z0-9_-]+$/;

/**
 * The signing token made it into a URL. Exported so tests (and the status
 * route) can sanity-check without holding a real token.
 */
export function isOctaveToken(value: string | undefined | null): boolean {
  return typeof value === "string" && TOKEN_RE.test(value);
}

const TOKEN_FILE = path.join(process.cwd(), "data", "octave-pbtoken.json");

function resolveScriptPath(): string {
  if (process.env.OCTAVE_PBTOKEN_SCRIPT) return process.env.OCTAVE_PBTOKEN_SCRIPT;
  const cwd = path.resolve(process.cwd(), "scripts", "get-pbtoken.mjs");
  if (existsSync(cwd)) return cwd;
  const sibling = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "..",
    "..",
    "scripts",
    "get-pbtoken.mjs",
  );
  if (existsSync(sibling)) return sibling;
  return cwd;
}

interface Cached { token: string; fetchedAt: number; expiresAt: number }

// Cache the token across restarts, like monochrome-session.json: relaunching
// Chromium on every boot is slow and can fail for reasons unrelated to the
// token having actually expired.
let cache: Cached | null = (() => {
  if (TOKEN_OVERRIDE) return null;
  try {
    const c = JSON.parse(readFileSync(TOKEN_FILE, "utf-8")) as Cached;
    if (c?.token && isOctaveToken(c.token) && Date.now() < c.expiresAt) return c;
  } catch {
    // No token on disk, or it is stale — first octave play refetches.
  }
  return null;
})();

function persistCache() {
  if (!cache) return;
  try {
    mkdirSync(path.dirname(TOKEN_FILE), { recursive: true });
    writeFileSync(TOKEN_FILE, JSON.stringify(cache));
  } catch {
    // A read-only data dir just means the token is refetched after a restart.
  }
}

function runScript(): Promise<string> {
  const script = resolveScriptPath();
  const bin = /(^|\/)(node|bun)(\.exe)?$/.test(process.execPath)
    ? process.execPath
    : "node";
  return new Promise((resolve, reject) => {
    const child = spawn(bin, [script], {
      cwd: process.cwd(),
      env: { ...process.env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    const timer = setTimeout(() => child.kill("SIGKILL"), SCRIPT_RUN_TIMEOUT_MS);
    child.stdout.on("data", (d: Buffer) => (out += d.toString()));
    child.stderr.on("data", (d: Buffer) => (out += d.toString()));
    child.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code !== 0) {
        reject(new Error(out.trim().slice(-400) || `get-pbtoken exited ${code}`));
        return;
      }
      resolve(out);
    });
  });
}

interface MintedToken {
  token: string;
  /** Absolute expiry in epoch seconds, when the upstream told us one. */
  exp: number | null;
  /** Upstream's clock-skew safety margin, in seconds. */
  skew: number | null;
}

/**
 * The script's stdout used to be a bare token, then a `octave:pbtoken = …`
 * line with a JSON envelope, and now a lone JSON line. Parse any of those and
 * surface the expiry so we don't hold a dead token for the whole local TTL.
 */
export function parseTokenOutput(out: string): MintedToken | null {
  // JSON envelope: { "token":"octk_…","exp":123,"skew":2400 }
  const jsonBlock = /(\{[\s\S]*\})/.exec(out)?.[1];
  if (jsonBlock) {
    try {
      const data = JSON.parse(jsonBlock) as {
        token?: unknown;
        exp?: unknown;
        skew?: unknown;
      };
      if (typeof data.token === "string" && isOctaveToken(data.token)) {
        return {
          token: data.token,
          exp: typeof data.exp === "number" ? data.exp : null,
          skew: typeof data.skew === "number" ? data.skew : null,
        };
      }
    } catch {
      // Not JSON — fall through to the bare-token scan.
    }
  }
  const bare = out.match(/octk_[A-Za-z0-9_-]+/);
  if (bare && isOctaveToken(bare[0])) return { token: bare[0], exp: null, skew: null };
  return null;
}

// Single-flight: a play that needs the token while a browser is already being
// driven waits on the same promise instead of launching a second one.
let inFlight: Promise<string> | null = null;
let lastFetchFailedAt = 0;

async function fetchToken(): Promise<string> {
  if (TOKEN_OVERRIDE) return TOKEN_OVERRIDE;
  if (inFlight) return inFlight;
  inFlight = runScript()
    .then((out) => {
      const minted = parseTokenOutput(out);
      if (!minted) {
        throw new Error(
          `no octk_ token in script output: ${out.trim().slice(-400)}`,
        );
      }
      // Trust the upstream's own expiry (net of its safety skew) when it told
      // us one, but never extend past our local cap — a token we can't reason
      // about should be refreshed sooner, not later.
      let expiresAt = Date.now() + TTL_MS;
      if (typeof minted.exp === "number") {
        const skewMs = typeof minted.skew === "number" ? minted.skew * 1000 : 0;
        const upstreamMs = minted.exp * 1000 - skewMs;
        expiresAt = Math.min(expiresAt, upstreamMs);
      }
      // A freshly minted token is usable for a least a minute; without this
      // floor, an upstream exp that's already in the past would make every
      // play relaunch the browser only to get another dead token.
      expiresAt = Math.max(expiresAt, Date.now() + 60_000);
      cache = { token: minted.token, fetchedAt: Date.now(), expiresAt };
      persistCache();
      lastFetchFailedAt = 0;
      return minted.token;
    })
    .catch((err) => {
      lastFetchFailedAt = Date.now();
      throw err;
    })
    .finally(() => {
      inFlight = null;
    });
  return inFlight;
}

/**
 * The current playback token, fetching it on first use if necessary. Returns
 * null when no token is (yet) available instead of throwing, so callers can
 * pick a fallback source quietly.
 */
export async function getOctaveToken(): Promise<string | null> {
  if (TOKEN_OVERRIDE) return TOKEN_OVERRIDE;
  if (cache && Date.now() < cache.expiresAt) return cache.token;
  if (Date.now() - lastFetchFailedAt < RETRY_COOLDOWN_MS) return null;
  try {
    return await fetchToken();
  } catch (e) {
    console.error("[octave] playback token fetch failed:", e instanceof Error ? e.message : e);
    return null;
  }
}

/** Drop any cached/on-disk token now — used when a stream comes back 401/403. */
export function invalidateOctaveToken(): void {
  cache = null;
  try {
    writeFileSync(TOKEN_FILE, JSON.stringify({ token: "", fetchedAt: 0, expiresAt: 0 }));
  } catch {
    // Best effort; the in-memory state is what matters.
  }
}

/** Force a refresh (status route / operator) and return the new token or null. */
export async function refreshOctaveToken(): Promise<string | null> {
  if (TOKEN_OVERRIDE) return TOKEN_OVERRIDE;
  cache = null;
  lastFetchFailedAt = 0;
  try {
    return await fetchToken();
  } catch (e) {
    console.error("[octave] playback token refresh failed:", e instanceof Error ? e.message : e);
    return null;
  }
}

export function octaveStatus(): {
  base: string;
  hasToken: boolean;
  source: "env" | "cache" | "none";
  fetchedAt: number | null;
  expiresAt: number | null;
  retryInMs: number;
} {
  const retryInMs = Math.max(0, lastFetchFailedAt + RETRY_COOLDOWN_MS - Date.now());
  return {
    base: octaveBase(),
    hasToken: !!TOKEN_OVERRIDE || (!!cache && Date.now() < cache.expiresAt),
    source: TOKEN_OVERRIDE ? "env" : cache ? "cache" : "none",
    fetchedAt: cache?.fetchedAt ?? null,
    expiresAt: cache?.expiresAt ?? null,
    retryInMs,
  };
}

/**
 * Build the signed MP3 URL for a Tidal track id. The token rides on the URL,
 * so this result is only ever fetched server-side in the stream proxy — it is
 * never returned to the client with the `k` intact.
 */
export function buildOctaveStreamUrl(trackId: string, token: string): string {
  const u = new URL(`${octaveBase()}/audio/320`);
  u.searchParams.set("track", trackId);
  u.searchParams.set("k", token);
  return u.toString();
}

// Octave has its own id space — it does NOT mirror Tidal's. Handing /audio/320
// a Tidal id costs ~15s and then 503s, every time, which is why octave never
// won a race. Their search maps title+artist to a real octave id in ~0.5s.
//
// Results carry no ISRC, so duration is the only precise discriminator: a
// search for "Dirtmouth" returns both the 115s track and a 322s edit of it.
interface OctaveSearchTrack {
  id?: string;
  title?: string;
  artist?: { name?: string };
  duration?: number;
}

/**
 * Tolerance when matching a track length against Octave's, in seconds.
 *
 * Services disagree by a second or two on the same recording (different
 * masters, different encoders). Wide enough to absorb that, narrow enough that
 * it still separates an album cut from a radio edit.
 */
const DURATION_SLACK_S = 5;

/**
 * Words that mean "a different recording", not "a different way of writing the
 * same title".
 *
 * This distinction is the whole game. "Sunflower (Spider-Man: Into the
 * Spider-Verse)" is the same song as "Sunflower" — the bracket is packaging.
 * "Heartbeat (OLIVER Remix)" is NOT the same song as "Heartbeat", even though
 * both are credited to Childish Gambino and both reduce to "heartbeat" once
 * the bracket is stripped. Treating those alike served a remix to someone who
 * asked for the album version.
 */
const VERSION_WORDS = [
  "remix", "live", "acoustic", "instrumental", "karaoke", "cover",
  "demo", "reprise", "edit", "version", "mix", "remaster", "remastered",
  "extended", "radio", "sped up", "slowed", "reverb", "mashup", "bootleg",
];

/** The version words present in a title, as a stable comparable key. */
function versionTags(value: string): string {
  const n = normalize(value);
  return VERSION_WORDS.filter((w) => new RegExp(`\\b${w}\\b`).test(n))
    .sort()
    .join(",");
}

// Negative results are cached too: a track Octave doesn't have would otherwise
// pay a search on every play, forever.
const idCache = new Map<string, string | null>();
const ID_CACHE_MAX = 500;

function normalize(value: string): string {
  return value
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Octave's id for a track, or null when it has no confident match.
 *
 * Null means "fall back to another source", never "guess" — a wrong match
 * plays a different song, which is worse than not playing from Octave at all.
 */
export async function resolveOctaveTrackId(
  meta: { title?: string; artist?: string; duration?: number },
  token: string,
): Promise<string | null> {
  const title = meta.title?.trim();
  const artist = meta.artist?.trim();
  if (!title) return null;

  const key = `${normalize(title)}|${normalize(artist ?? "")}|${meta.duration ?? ""}`;
  const cached = idCache.get(key);
  if (cached !== undefined) return cached;

  let match: string | null = null;
  try {
    const u = new URL(`${octaveBase()}/api/search`);
    u.searchParams.set("q", artist ? `${title} ${artist}` : title);
    u.searchParams.set("k", token);
    const res = await fetch(u.toString(), {
      headers: { accept: "application/json" },
      signal: AbortSignal.timeout(SEARCH_TIMEOUT_MS),
    });
    if (res.ok) {
      const body = (await res.json()) as { tracks?: OctaveSearchTrack[] };
      match = pickBestTrack(body.tracks ?? [], title, artist, meta.duration);
    } else {
      await res.body?.cancel();
    }
  } catch {
    // A search that fails is just a miss; the race falls through to another
    // source rather than failing the play.
  }

  if (idCache.size >= ID_CACHE_MAX) {
    idCache.delete(idCache.keys().next().value as string);
  }
  idCache.set(key, match);
  return match;
}

/**
 * The title without a trailing bracketed qualifier or " - …" tail.
 *
 * Octave and Tidal disagree constantly on these: the real Post Malone track is
 * "Sunflower (Spider-Man: Into the Spider-Verse)" on Octave and often just
 * "Sunflower" upstream. Comparing the bare titles is what stops that being a
 * false miss.
 */
function baseTitle(value: string): string {
  return normalize(value.replace(/\s*[([].*$/, "").replace(/\s+-\s+.*$/, ""));
}

/**
 * The credited lead, for artist comparison.
 *
 * Upstream tends to list every collaborator ("Post Malone, Swae Lee") where
 * Octave credits only the lead, so comparing full strings drops real matches.
 */
function primaryArtist(value: string): string {
  return normalize(
    value
      .split(/[,&]|\bfeat\.?\b|\bft\.?\b|\bwith\b/i)[0] ?? value,
  );
}

function pickBestTrack(
  tracks: OctaveSearchTrack[],
  title: string,
  artist: string | undefined,
  duration: number | undefined,
): string | null {
  const wantTitle = normalize(title);
  const wantBase = baseTitle(title);
  const wantTags = versionTags(title);
  const wantArtist = artist ? primaryArtist(artist) : "";

  let best: { id: string; score: number } | null = null;

  for (const t of tracks) {
    if (!t.id || !t.title) continue;

    // The lead artist is the one hard gate. Octave's search is full of covers,
    // karaoke and piano arrangements of the exact title being asked for — and
    // the real track is often not the first result — so anything that doesn't
    // credit the same lead is rejected outright.
    if (wantArtist && primaryArtist(t.artist?.name ?? "") !== wantArtist) continue;

    // Length is the other hard gate: it separates the album cut from a live
    // version, an extended edit or a radio trim of the same name by the same
    // artist.
    if (
      typeof duration === "number" &&
      typeof t.duration === "number" &&
      Math.abs(t.duration - duration) > DURATION_SLACK_S
    ) {
      continue;
    }

    // Third hard gate: both sides must be the same *kind* of recording. A
    // remix, live cut or acoustic take is a different track, and stripping the
    // bracket would otherwise make it indistinguishable from the original.
    if (versionTags(t.title) !== wantTags) continue;

    const got = normalize(t.title);
    let score: number;
    if (got === wantTitle) score = 3;
    else if (baseTitle(t.title) === wantBase) score = 2;
    else continue;

    if (!best || score > best.score) best = { id: t.id, score };
    if (score === 3) break;
  }

  return best?.id ?? null;
}

/** Absolute path to the token-minting script, for error messages. */
export function octaveScriptPath(): string {
  return resolveScriptPath();
}