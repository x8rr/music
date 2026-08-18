import { FastifyInstance, FastifyReply } from "fastify";
import { Readable } from "node:stream";
import { promises as fsp, constants as fsConstants } from "node:fs";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { tidalSearchTracks } from "../lib/tidal";
import {
  resolveMonochromeStream,
  isAuthed as monochromeIsAuthed,
  authState as monochromeAuthState,
  exchangeTurnstile as monochromeExchangeTurnstile,
  getConfig as monochromeGetConfig,
  AUTH_PAGE_HTML as MONOCHROME_AUTH_PAGE,
} from "../lib/monochromeTrack";
import musicSources from "../config/music-sources.json";
import {
  buildOctaveStreamUrl,
  getOctaveToken,
  invalidateOctaveToken,
  octaveScriptPath,
  octaveStatus,
  refreshOctaveToken,
  resolveOctaveTrackId,
} from "../lib/octave";

/**
 * Release a response we're not going to read.
 *
 * An un-consumed `Response` body keeps its socket and the buffers behind it
 * alive until GC gets around to it. On the happy path that never matters,
 * because the success branch reads the body via .json()/.text(). It matters a
 * great deal on error paths: when the upstreams are unhealthy, every play
 * cascades through five providers and each failed leg abandons a live body.
 * That is a slow OOM that only shows up when something else is already broken.
 */
function discard(res: { body?: { cancel(): Promise<void> } | null }): void {
  res.body?.cancel().catch(() => {});
}

// Instance lists live in music-sources.json — edit that file to add/remove/
// reorder mirrors (first entry wins) without touching code. An env var with
// the same name still overrides the whole list (comma-separated) if set, for
// per-deployment tweaks without editing the checked-in file.
function sourceBases(envVar: string, fallback: string[]): string[] {
  const raw = process.env[envVar];
  if (!raw) return fallback;
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

const MUSIC_API_BASES = sourceBases(
  "MUSIC_API_BASES",
  musicSources.musicApiBases,
);
const MUSIC_STREAM_BASES = sourceBases(
  "MUSIC_STREAM_BASES",
  musicSources.musicStreamBases,
);
const QOBUZ_BASES = sourceBases("QOBUZ_API_BASES", musicSources.qobuzBases);
const DEEZER_BASES = sourceBases("DEEZER_API_BASES", musicSources.deezerBases);
// The standalone yt-dlp-backed SoundCloud service (soundcloud-backend/). It
// reaches SoundCloud through yt-dlp rather than the api-v2 client_id flow, so
// it keeps working when the scraped client_id is dead or rate-limited — which
// is the whole reason it exists as a separate fallback from "soundcloud".
const SCDLP_BASES = sourceBases("SC_DLP_BASES", musicSources.scdlpBases);

const AUDIO_PROXY_BASE =
  process.env.AUDIO_PROXY_BASE || "https://audio-proxy.binimum.org/proxy-audio";

// The audio proxy is a third party we can't renew certs for, and its cert has
// lapsed before. A browser can't be told to ignore that — the click-through
// interstitial only exists for top-level navigations, so subresource fetches
// just fail — which takes every Tidal/Qobuz stream down while YouTube (which
// doesn't route through it) keeps working.
//
// So we relay segments through /api/music/segment instead of pointing the
// player straight at the proxy: fetched here, re-served over our own cert.
// Verification is ON unless someone explicitly opts out with
// AUDIO_PROXY_INSECURE=1, so a fresh deploy is safe by default and accepting a
// bad cert is a deliberate act rather than something you inherit.
//
// This relay is only reached for Tidal DASH playback, and Tidal currently
// resolves to 30-second previews that assertPlayable rejects — so nothing hits
// this path today and the strict default costs no functionality. If the Tidal
// mirrors come back and the proxy's cert is still expired, that is the moment
// to weigh setting this, knowing an attacker in between could substitute audio.
const AUDIO_PROXY_INSECURE = process.env.AUDIO_PROXY_INSECURE === "1";

const SEGMENT_PREFIX = "/api/music/segment/";

const MUSIC_TIMEOUT_MS = 4000;

const DEEZER_ORIGIN = process.env.DEEZER_ORIGIN || "https://monochrome.tf";

const DEEZER_TIMEOUT_MS = Number(process.env.DEEZER_TIMEOUT_MS) || 4000;

function isDeezerUrl(url: string): boolean {
  return DEEZER_BASES.some((b) => url.startsWith(b.replace(/\/+$/, "")));
}

function streamFetchHeaders(
  url: string,
  base: Record<string, string> = {},
): Record<string, string> {
  if (isDeezerUrl(url))
    return { ...base, Origin: DEEZER_ORIGIN, Referer: `${DEEZER_ORIGIN}/` };
  return base;
}

interface TidalArtist {
  id: number;
  name: string;
  picture?: string | null;
  type?: string;
}

interface TidalAlbum {
  id: number;
  title: string;
  cover?: string | null;
  vibrantColor?: string | null;
}

interface TidalTrack {
  id: number;
  title: string;
  duration: number;
  version?: string | null;
  explicit?: boolean;
  copyright?: string;
  popularity?: number;
  audioQuality?: string;
  artist?: TidalArtist;
  artists?: TidalArtist[];
  album?: TidalAlbum;
  isrc?: string;
}

interface TidalSearchResp {
  data?: {
    items?: TidalTrack[];
    totalNumberOfItems?: number;
  };
}

interface ClientTrack {
  id: number | string;
  title: string;
  artist: string;
  artistId?: number;
  album?: string;
  albumId?: number;
  artwork: string;
  duration: number;
  explicit?: boolean;
  isrc?: string;
  isDash?: boolean;
  source?: SourcePriority;
  permalinkUrl?: string;
}

interface ClientAlbum {
  id: number;
  title: string;
  cover: string;
  artist: { id: number; name: string };
  releaseDate?: string;
  numberOfTracks?: number;
  type?: string;
}

interface ClientArtist {
  id: number;
  name: string;
  picture: string;
}

interface SoundcloudTrack {
  id: number;
  title: string;
  user: {
    username: string;
    avatar_url?: string;
    permalink_url?: string;
  };
  artwork_url?: string;
  duration: number;
  explicit?: boolean;
  isrc?: string | null;
  permalink_url: string;
  stream_url?: string;
  tag_list?: string;
  description?: string;
  genre?: string;
  release_year?: number;
  play_count?: number;
  likes_count?: number;
  reposts_count?: number;
}

interface SoundcloudTranscoding {
  url: string;
  format: { protocol: "progressive" | "hls"; mime_type: string };
  quality?: string;
}

interface SoundcloudTrackInfo extends SoundcloudTrack {
  stream_url?: string;
  downloadable?: boolean;
  download_url?: string;
  media?: { transcodings?: SoundcloudTranscoding[] };
}

interface SoundcloudSearchResp {
  collection?: SoundcloudTrack[];
  total_results?: number;
}

interface SoundcloudPlaylist {
  id: number;
  title: string;
  user: {
    username: string;
    avatar_url?: string;
  };
  tracks: SoundcloudTrack[];
  track_count: number;
  artwork_url?: string;
  permalink_url: string;
  description?: string;
}

interface QobuzSearchResp {
  success: boolean;
  data?: {
    tracks?: {
      items?: Array<{
        id: number;
        title: string;
        artist?: { name: string };
        artists?: Array<{ name: string }>;
        album?: { title: string; cover?: string };
        duration: number;
        explicit?: boolean;
        isrc?: string;
      }>;
    };
  };
}

interface QobuzDownloadResp {
  success: boolean;
  url?: string;
  stream_url?: string;
  data?: {
    url?: string;
    stream_url?: string;
  };
}

type SourcePriority =
  | "tidal"
  | "octave"
  | "monochrome"
  | "qobuz"
  | "scdlp"
  | "deezer"
  | "soundcloud"
  | "youtube";

/** Sources that resolve a stream but have no search endpoint of their own —
 * they can only answer "give me audio for this title/artist/ISRC". They're
 * excluded from search ordering and from the health ping. */
const STREAM_ONLY_SOURCES: ReadonlySet<SourcePriority> = new Set([
  "octave",
  "deezer",
  "monochrome",
]);

/** Canonical source order. Streaming races all of these at once and plays
 * whichever resolves first, so this order only decides search fallback and
 * ties in the UI — but it is the one place the intended ranking is written
 * down, so keep it in sync with the picker in Music.tsx. */
const SOURCE_ORDER: SourcePriority[] = [
  "tidal",
  "octave",
  "monochrome",
  "qobuz",
  "scdlp",
  "deezer",
  "soundcloud",
  "youtube",
];

let currentSourcePriority: SourcePriority = "tidal";
const sourceLatencies: Map<SourcePriority, number> = new Map();
let lastPingTime = 0;

/* Stream proxy instrumentation, read by /api/system/memory.
 *
 * `open` is the number of upstream audio streams currently being piped. If it
 * climbs and never comes back down, streams are not being torn down when the
 * client goes away — which is exactly the shape of a native memory leak that
 * the JS heap cannot see, because the buffered audio lives in the stream
 * plumbing rather than in any JS object we hold. */
const streamProxy = { open: 0, started: 0, finished: 0 };
export function musicStats() {
  return { ...streamProxy };
}
const PING_INTERVAL = 300000;

let cachedClientId: string | null = null;
let clientIdExpiry: number = 0;

function tidalCoverUrl(cover: string | null | undefined, size = 640): string {
  if (!cover) return "";
  return `https://resources.tidal.com/images/${cover.replace(/-/g, "/")}/${size}x${size}.jpg`;
}

function tidalArtistPictureUrl(
  picture: string | null | undefined,
  size = 320,
): string {
  if (!picture) return "";
  return `https://resources.tidal.com/images/${picture.replace(/-/g, "/")}/${size}x${size}.jpg`;
}

function toClientAlbum(a: any): ClientAlbum {
  const artist =
    a.artist || (Array.isArray(a.artists) ? a.artists[0] : null) || {};
  return {
    id: a.id,
    title: a.title || "Unknown Album",
    cover: tidalCoverUrl(a.cover),
    artist: { id: artist.id || 0, name: artist.name || "Unknown" },
    releaseDate: a.releaseDate,
    numberOfTracks: a.numberOfTracks,
    type: a.type,
  };
}

function toClientArtist(a: any): ClientArtist {
  return {
    id: a.id,
    name: a.name || "Unknown Artist",
    picture: tidalArtistPictureUrl(a.picture),
  };
}

function extractSearchSection(data: any, key: string): any[] {
  if (!data || typeof data !== "object") return [];

  if (data.data && typeof data.data === "object") {
    return extractSearchSection(data.data, key);
  }

  if (data[key] && Array.isArray((data[key] as any).items)) {
    return (data[key] as any).items;
  }

  if (key === "tracks" && Array.isArray(data.items)) return data.items;

  return [];
}

function soundcloudArtworkUrl(url: string | undefined, size = 500): string {
  if (!url) return "";
  return url.replace(/-(t500x500|large|original)/, `-t${size}x${size}`);
}

function musicError(
  reply: FastifyReply,
  status: number,
  message: string,
  extra?: Record<string, unknown>,
) {
  reply.code(status);
  return reply.send({ error: message, ...extra });
}

// Octave signs its stream URLs with a shared server-side token. The client
// never plays an octave URL directly — it always goes through the /api/music
// stream proxy — so redact it from any JSON we do hand back.
function redactOctaveToken(url: string): string {
  try {
    const u = new URL(url);
    if (u.searchParams.has("k")) {
      u.searchParams.set("k", "redacted");
      return u.toString();
    }
  } catch {
    // Not a URL; leave it alone.
  }
  return url;
}

const SC_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

const SC_CLIENT_ID_RE = /client_id\s*[:=]\s*\\?"?([a-zA-Z0-9]{20,})/;

async function scFetchText(url: string): Promise<string> {
  const res = await fetch(url, { headers: { "User-Agent": SC_UA } });
  return res.text();
}

/** A client_id is valid if the API doesn't reject it as unauthorized. */
async function clientIdWorks(id: string): Promise<boolean> {
  try {
    const u = new URL("https://api-v2.soundcloud.com/resolve");
    u.searchParams.set("url", "https://soundcloud.com/discover");
    u.searchParams.set("client_id", id);
    const r = await fetch(u.toString(), { headers: { "User-Agent": SC_UA } });
    return r.status !== 401 && r.status !== 403;
  } catch {
    return false;
  }
}

async function getSoundcloudClientId(): Promise<string> {
  const fallbackId = process.env.SOUNDCLOUD_CLIENT_ID;
  if (fallbackId) return fallbackId;

  try {
    const html = await scFetchText("https://soundcloud.com/");

    const assetUrls = [
      ...html.matchAll(
        /src="(https:\/\/a-v2\.(?:sndcdn|soundcloud)\.com\/assets\/[^"]+\.js)"/g,
      ),
    ]
      .map((m) => m[1])
      .reverse();

    let firstFound: string | null = null;
    for (const jsUrl of assetUrls) {
      const js = await scFetchText(jsUrl);
      const m = js.match(SC_CLIENT_ID_RE);
      if (!m) continue;
      const id = m[1];
      if (firstFound === null) firstFound = id;
      // Prefer an id we can confirm still works; otherwise keep the first seen.
      if (await clientIdWorks(id)) return id;
    }
    if (firstFound) return firstFound;

    throw new Error("Could not extract client_id");
  } catch (error) {
    console.error("Failed to get SoundCloud client_id:", error);
    throw new Error("No SoundCloud client_id available");
  }
}

async function getValidClientId(): Promise<string> {
  if (cachedClientId && Date.now() < clientIdExpiry) {
    return cachedClientId;
  }
  cachedClientId = await getSoundcloudClientId();
  clientIdExpiry = Date.now() + 3600000;
  return cachedClientId;
}

async function soundcloudApiRequest<T>(
  path: string,
  params: Record<string, string | number | undefined> = {},
): Promise<T> {
  const clientId = await getValidClientId();
  const url = new URL(path, "https://api-v2.soundcloud.com");

  url.searchParams.set("client_id", clientId);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined) url.searchParams.set(k, String(v));
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), MUSIC_TIMEOUT_MS);

  try {
    const response = await fetch(url.toString(), {
      signal: controller.signal,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        Accept: "application/json, */*",
        Origin: "https://soundcloud.com",
        Referer: "https://soundcloud.com/",
      },
    });
    clearTimeout(timeout);

    if (!response.ok) {
      if (response.status === 401 || response.status === 403) {
        cachedClientId = null;
      }
      throw new Error(`SoundCloud API error: ${response.status}`);
    }

    return (await response.json()) as T;
  } catch (error) {
    clearTimeout(timeout);
    throw error;
  }
}

function toSoundcloudClientTrack(track: SoundcloudTrack): ClientTrack {
  return {
    id: track.id,
    title: track.title,
    artist: track.user.username,
    album: track.genre || undefined,
    artwork: soundcloudArtworkUrl(track.artwork_url || track.user.avatar_url),
    duration: Math.floor(track.duration / 1000),
    explicit: track.explicit || false,
    isrc: track.isrc || undefined,
    source: "soundcloud",
    permalinkUrl: track.permalink_url,
  };
}

function toClientTrack(t: TidalTrack): ClientTrack {
  const primaryArtist =
    t.artists && t.artists.length > 0 ? t.artists[0] : t.artist;
  const artist =
    (t.artists && t.artists.length > 0
      ? t.artists.map((a) => a.name).join(", ")
      : t.artist?.name) || "Unknown";
  return {
    id: t.id,
    title: t.title + (t.version ? ` (${t.version})` : ""),
    artist,
    artistId: primaryArtist?.id,
    album: t.album?.title,
    albumId: t.album?.id,
    artwork: tidalCoverUrl(t.album?.cover),
    duration: t.duration,
    explicit: t.explicit,
    isrc: t.isrc,
    source: "tidal",
    isDash: true,
  };
}

async function callMusicApi<T>(
  path: string,
  params: Record<string, string | number | undefined> = {},
  bases: string[] = MUSIC_API_BASES,
): Promise<T> {
  const makeRequest = async (base: string): Promise<T> => {
    const url = new URL(path, base);
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined) url.searchParams.set(k, String(v));
    }
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), MUSIC_TIMEOUT_MS);
    try {
      const r = await fetch(url, {
        signal: ctrl.signal,
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
          Accept: "application/json, */*",
          "Accept-Language": "en-US,en;q=0.9",
          Origin: "https://listen.tidal.com",
          Referer: "https://listen.tidal.com/",
        },
      });
      clearTimeout(t);
      if (!r.ok) {
        discard(r);
        throw new Error(`HTTP ${r.status}`);
      }
      return (await r.json()) as T;
    } catch (e) {
      clearTimeout(t);
      throw e;
    }
  };

  return new Promise<T>((resolve, reject) => {
    let settled = false;
    let remaining = bases.length;
    const errors: string[] = [];

    for (const base of bases) {
      makeRequest(base).then(
        (result) => {
          if (!settled) {
            settled = true;
            resolve(result);
          }
        },
        (err) => {
          errors.push(
            `${base}: ${err instanceof Error ? err.message : String(err)}`,
          );
          remaining--;
          if (remaining === 0 && !settled) {
            reject(
              new Error(`All music upstreams failed: ${errors.join(" | ")}`),
            );
          }
        },
      );
    }
  });
}

async function qobuzSearch(
  query: string,
  limit: number,
): Promise<ClientTrack[]> {
  for (const base of QOBUZ_BASES) {
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), MUSIC_TIMEOUT_MS);
      const res = await fetch(
        `${base}/api/get-music?q=${encodeURIComponent(query)}&offset=0&limit=${limit}`,
        { signal: ctrl.signal },
      );
      clearTimeout(t);

      if (!res.ok) {
        discard(res);
        continue;
      }

      const data = (await res.json()) as QobuzSearchResp;

      const items = data?.data?.tracks?.items || [];
      return items.map((t): ClientTrack => ({
        id: t.id,
        title: t.title,
        artist: t.artist?.name || t.artists?.[0]?.name || "Unknown",
        album: t.album?.title,
        artwork: t.album?.cover || "",
        duration: t.duration,
        explicit: t.explicit || false,
        isrc: t.isrc,
        source: "qobuz",
      }));
    } catch {
      continue;
    }
  }
  return [];
}

async function resolveQobuzStreamUrl(isrc: string): Promise<{ url: string }> {
  const errors: Array<{ base: string; reason: string }> = [];

  for (const base of QOBUZ_BASES) {
    try {
      const searchCtrl = new AbortController();
      const t1 = setTimeout(() => searchCtrl.abort(), MUSIC_TIMEOUT_MS);
      const searchRes = await fetch(
        `${base}/api/get-music?q=${encodeURIComponent(isrc)}&offset=0`,
        { signal: searchCtrl.signal },
      );
      clearTimeout(t1);
      if (!searchRes.ok) {
        const body = await searchRes.text().catch(() => "");
        errors.push({
          base,
          reason: `search HTTP ${searchRes.status}: ${body}`,
        });
        continue;
      }
      const searchData = (await searchRes.json()) as QobuzSearchResp;
      const trackId = searchData?.data?.tracks?.items?.[0]?.id;
      if (!trackId) {
        errors.push({ base, reason: "no track found for ISRC" });
        continue;
      }

      const dlCtrl = new AbortController();
      const t2 = setTimeout(() => dlCtrl.abort(), MUSIC_TIMEOUT_MS);
      const dlRes = await fetch(
        `${base}/api/download-music?track_id=${trackId}&quality=6`,
        { signal: dlCtrl.signal },
      );
      clearTimeout(t2);
      if (!dlRes.ok) {
        const body = await dlRes.text().catch(() => "");
        errors.push({
          base,
          reason: `download HTTP ${dlRes.status}: ${body}`,
        });
        continue;
      }
      const dlData = (await dlRes.json()) as QobuzDownloadResp;
      const url = dlData?.data?.url ?? dlData?.url ?? dlData?.stream_url;
      if (!url) {
        errors.push({ base, reason: "no URL in download-music response" });
        continue;
      }
      return { url };
    } catch (e) {
      const reason =
        e instanceof Error
          ? e.name === "AbortError"
            ? `timed out after ${MUSIC_TIMEOUT_MS}ms`
            : e.message
          : String(e);
      errors.push({ base, reason });
    }
  }

  const summary = errors.map((e) => `${e.base} → ${e.reason}`).join(" | ");
  throw new Error(`All Qobuz upstreams failed: ${summary}`);
}

// Health probes run under Promise.all, so one hanging upstream stalls the whole
// ping — and /api/music/source and /api/music/ping along with it. A dead mirror
// that accepts the connection and never answers (qobuz.kennyy.com.br does
// exactly this) would otherwise hold the request open indefinitely.
const PING_REQUEST_TIMEOUT_MS = 3000;
const PING_TOTAL_TIMEOUT_MS = 8000;

/** A bounded fetch for probes. */
async function pingFetch(
  url: string,
  init: RequestInit = {},
): Promise<Response> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), PING_REQUEST_TIMEOUT_MS);
  try {
    return await fetch(url, { ...init, signal: ctrl.signal });
  } finally {
    clearTimeout(timer);
  }
}

/** Caps a probe that can't be given an abort signal — tidalSearchTracks and
 * yt-search both wrap their own transports. The underlying work may keep
 * running, but the ping stops waiting on it. */
function withDeadline<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([
    p,
    new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`${label} ping exceeded ${ms}ms`)),
        ms,
      );
    }),
  ]).finally(() => clearTimeout(timer)) as Promise<T>;
}

async function testSourceLatency(source: SourcePriority): Promise<number> {
  const start = Date.now();
  try {
    await withDeadline(probeSource(source), PING_TOTAL_TIMEOUT_MS, source);
    const latency = Date.now() - start;
    sourceLatencies.set(source, latency);
    return latency;
  } catch (err) {
    console.log(`${source} ping failed:`, err);
    sourceLatencies.set(source, Infinity);
    return Infinity;
  }
}

async function probeSource(source: SourcePriority): Promise<void> {
  {
    switch (source) {
      case "tidal": {
        await tidalSearchTracks("test", 1);
        break;
      }
      case "qobuz": {
        let success = false;
        for (const base of QOBUZ_BASES) {
          try {
            const res = await pingFetch(
              `${base}/api/get-music?q=test&offset=0&limit=1`,
            );
            if (res.ok) {
              success = true;
              break;
            }
          } catch {
            continue;
          }
        }
        if (!success) throw new Error("No Qobuz base responded");
        break;
      }
      case "soundcloud": {
        const clientId = await getValidClientId();
        const url = new URL("https://api-v2.soundcloud.com/tracks");
        url.searchParams.set("client_id", clientId);
        url.searchParams.set("q", "test");
        url.searchParams.set("limit", "1");
        const res = await pingFetch(url.toString(), {
          headers: {
            "User-Agent": "Mozilla/5.0",
            Accept: "application/json",
          },
        });
        if (!res.ok) {
          discard(res);
          throw new Error(`SoundCloud HTTP ${res.status}`);
        }
        break;
      }
      case "scdlp": {
        if (SCDLP_BASES.length === 0) throw new Error("No scdlp base");
        let success = false;
        for (const base of SCDLP_BASES) {
          try {
            const res = await pingFetch(
              `${base.replace(/\/+$/, "")}/api/music/status`,
            );
            if (!res.ok) {
              discard(res);
              continue;
            }
            const data = (await res.json()) as { ok?: boolean };
            if (data.ok) {
              success = true;
              break;
            }
          } catch {
            continue;
          }
        }
        if (!success) throw new Error("No scdlp base responded");
        break;
      }
      case "youtube": {
        const { default: yts } = await import("yt-search");
        await (yts as (q: string) => Promise<unknown>)("test audio");
        break;
      }
    }
  }
}

let pingInFlight = false;

function schedulePing() {
  const now = Date.now();
  if (pingInFlight) return;
  if (now - lastPingTime < PING_INTERVAL && sourceLatencies.size > 0) return;
  pingInFlight = true;
  pingAllSources().finally(() => {
    pingInFlight = false;
  });
}

async function pingAllSources() {
  const now = Date.now();
  lastPingTime = now;

  try {
    await getValidClientId();
  } catch (e) {
    console.log("SoundCloud client ID unavailable:", e);
    sourceLatencies.set("soundcloud", Infinity);
  }

  const pinged = SOURCE_ORDER.filter((s) => !STREAM_ONLY_SOURCES.has(s));
  const results = await Promise.all(pinged.map(testSourceLatency));

  const workingSources: SourcePriority[] = [];
  const sourceLatencyMap: Record<string, number> = {};

  results.forEach((lat, idx) => {
    const source = pinged[idx];
    sourceLatencyMap[source] = lat;
    if (lat !== Infinity) {
      workingSources.push(source);
    }
  });

  const preferred = pinged.find((s) => workingSources.includes(s));
  if (preferred) currentSourcePriority = preferred;

  console.log(
    `[SourcePing] Current: ${currentSourcePriority}, Latencies:`,
    sourceLatencyMap,
  );
}

async function searchWithPriority(
  query: string,
  limit: number,
  requestedSource?: SourcePriority,
): Promise<ClientTrack[]> {
  schedulePing();

  const sourceToUse = requestedSource || currentSourcePriority;

  // Deezer resolves streams by ISRC only — it has no search of its own.
  const fallbackOrder: SourcePriority[] = SOURCE_ORDER.filter(
    (s) => !STREAM_ONLY_SOURCES.has(s),
  );
  const startIndex = Math.max(0, fallbackOrder.indexOf(sourceToUse));
  const orderedSources = [
    ...fallbackOrder.slice(startIndex),
    ...fallbackOrder.slice(0, startIndex),
  ];

  for (const source of orderedSources) {
    try {
      let results: ClientTrack[] = [];

      if (source === "tidal") {
        let items: TidalTrack[] | undefined;
        try {
          items = (await tidalSearchTracks(query, limit)).items as TidalTrack[];
        } catch {
          const data = await callMusicApi<TidalSearchResp>("/search/", {
            s: query,
            limit: limit,
          });
          items = data?.data?.items;
        }
        results = (items || []).map(toClientTrack);
      } else if (source === "qobuz") {
        results = await qobuzSearch(query, limit);
      } else if (source === "soundcloud") {
        const data = await soundcloudApiRequest<SoundcloudSearchResp>(
          "/tracks",
          {
            q: query,
            limit: limit,
          },
        );
        results = (data?.collection || []).map(toSoundcloudClientTrack);
      } else if (source === "scdlp") {
        results = await scdlpSearch(query, limit);
      } else if (source === "youtube") {
        results = await ytSearchAccurate(query, limit);
      }

      if (results.length > 0) {
        return results;
      }
    } catch (error) {
      console.error(`${source} search failed:`, error);
      continue;
    }
  }

  return [];
}

interface StreamResult {
  url: string;
  mimeType: string;
  source: string;
  rawDash?: string;
  isDash?: boolean;
}

interface StreamMeta {
  title?: string;
  artist?: string;
  /** Real track length in seconds, from search results. Lets us recognise a
   * 30-second preview as a failure rather than a stream. */
  duration?: number;
}

async function resolveFromSource(
  source: SourcePriority,
  id: string,
  isrc: string | undefined,
  quality: string | undefined,
  sourceHint: SourcePriority | undefined,
  meta: StreamMeta | undefined,
  octaveToken?: string,
): Promise<StreamResult> {
  if (source === "tidal") {
    const r = await resolveTidalStreamUrl(id, quality ?? "HIGH");
    return {
      url: r.url,
      mimeType: r.mimeType || "application/dash+xml",
      source: "tidal",
      rawDash: r.rawDash,
      isDash: r.isDash,
    };
  }
  if (source === "octave") {
    // Kept as-is: octave still only races for Tidal-sourced tracks, or when the
    // user picked "Octave" in the UI. Failing fast prevents it from burning a
    // (slow) browser launch to fetch a token it won't get to use.
    if (sourceHint !== "tidal" && sourceHint !== "octave")
      throw new Error("Octave is only raced for Tidal-sourced tracks");
    const token = octaveToken || (await getOctaveToken());
    if (!token) throw new Error("No Octave playback token");
    // `id` is Tidal's, and Octave's id space is unrelated to it — passing it
    // straight through made /audio/320 spend ~15s looking for a track that
    // isn't there and then 503, on every single play. Resolve a real octave id
    // by title/artist/duration first; a miss falls through to another source.
    const octaveId = await resolveOctaveTrackId(
      { title: meta?.title, artist: meta?.artist, duration: meta?.duration },
      token,
    );
    if (!octaveId) throw new Error("No Octave match for this track");
    return {
      url: buildOctaveStreamUrl(octaveId, token),
      mimeType: "audio/mpeg",
      source: "octave",
    };
  }
  if (source === "qobuz") {
    if (!isrc) throw new Error("No ISRC for Qobuz lookup");
    const r = await resolveQobuzStreamUrl(isrc);
    return { url: r.url, mimeType: "audio/flac", source: "qobuz" };
  }
  if (source === "monochrome") {
    const r = await resolveMonochromeStream({
      title: meta?.title,
      artist: meta?.artist,
      isrc,
      duration: meta?.duration,
    });
    return { url: r.url, mimeType: "audio/flac", source: "monochrome" };
  }
  if (source === "scdlp") {
    const r = await resolveScDlpStreamUrl(id, sourceHint, meta);
    return { url: r.url, mimeType: r.mimeType, source: "scdlp" };
  }
  if (source === "deezer") {
    if (!isrc) throw new Error("No ISRC for Deezer lookup");
    const r = await resolveDeezerStreamUrl(isrc, quality);
    return { url: r.url, mimeType: r.mimeType, source: "deezer" };
  }
  if (source === "soundcloud") {
    const numId = parseInt(id);
    let scTrackId: number;
    if (sourceHint !== "soundcloud" && meta?.artist && meta?.title) {
      const scResults = await soundcloudApiRequest<SoundcloudSearchResp>(
        "/tracks",
        {
          q: `${meta.artist} ${meta.title}`,
          limit: 5,
        },
      );
      const match =
        scResults.collection?.find((t) =>
          t.title
            .toLowerCase()
            .includes(meta.title!.toLowerCase().split("(")[0].trim()),
        ) || scResults.collection?.[0];
      if (!match) throw new Error("No SoundCloud results for this track");
      scTrackId = match.id;
    } else if (!isNaN(numId)) {
      scTrackId = numId;
    } else {
      throw new Error("No SoundCloud track ID or search metadata available");
    }
    const r = await resolveSoundcloudStreamUrl(scTrackId);
    return { url: r.url, mimeType: r.mimeType, source: "soundcloud" };
  }

  if (meta?.artist && meta?.title) {
    const picked = await pickYoutubeMatch(meta.artist, meta.title);
    if (!picked) throw new Error("No YouTube match");
    const r = await resolveYoutubeStreamUrl(picked);
    return { url: r.url, mimeType: r.mimeType, source: "youtube" };
  }
  const r = await resolveYoutubeStreamUrl(id);
  return { url: r.url, mimeType: r.mimeType, source: "youtube" };
}

// YouTube's default search sorts by algorithmic relevance and freely mixes in
// covers, reactions, sped-up remixes, and hour-long compilations. When we're
// using YouTube as an audio fallback for a specific known song we don't want
// any of that. Pick the video that best matches the requested artist+title.
const YT_JUNK_TERMS = [
  "reaction",
  "review",
  "cover ",
  " cover",
  "sped up",
  "slowed",
  "nightcore",
  "8d audio",
  "1 hour",
  "one hour",
  "10 hour",
  "loop",
  "mashup",
  "karaoke",
  "instrumental",
];

interface YtVideoLike {
  videoId: string;
  title: string;
  author?: { name?: string };
  duration?: { seconds?: number };
  views?: number;
}

function scoreYtVideo(
  v: YtVideoLike,
  wantArtist: string,
  wantTitle: string,
  wantDurationSec: number | null,
): number {
  const title = (v.title || "").toLowerCase();
  const author = (v.author?.name || "").toLowerCase();
  const artist = wantArtist.toLowerCase();
  const song = wantTitle.toLowerCase();

  if (YT_JUNK_TERMS.some((k) => title.includes(k))) return -Infinity;

  let s = 0;
  // Title must contain the song name; strongly preferred that the artist
  // shows up somewhere too (title or channel).
  if (title.includes(song)) s += 40;
  if (author.includes(artist)) s += 30;
  else if (title.includes(artist)) s += 15;

  // "Topic" channels and channels with "VEVO" / "Official" in the name are
  // usually the label's own uploads — highest-quality masters.
  if (/ - topic$/.test(author)) s += 25;
  if (/(vevo|official)/i.test(v.author?.name || "")) s += 20;
  if (/(official (audio|music video|video|lyric)|audio only)/i.test(v.title || ""))
    s += 15;

  // Match duration within ±5s when we know it (Tidal/Qobuz always give us
  // one). Anything wildly off is almost certainly the wrong upload.
  if (wantDurationSec && v.duration?.seconds) {
    const diff = Math.abs(v.duration.seconds - wantDurationSec);
    if (diff <= 3) s += 30;
    else if (diff <= 8) s += 15;
    else if (diff > 30) s -= 40;
    if (v.duration.seconds > 15 * 60) s -= 80; // hour-long uploads
  }

  return s;
}

async function ytSearchAccurate(
  query: string,
  limit: number,
): Promise<ClientTrack[]> {
  const { default: yts } = await import("yt-search");
  const raw = (await (yts as unknown as (q: string) => Promise<{
    videos?: YtVideoLike[];
  }>)(query)).videos ?? [];
  const filtered = raw.filter(
    (v) =>
      v.videoId &&
      !YT_JUNK_TERMS.some((k) => (v.title || "").toLowerCase().includes(k)),
  );
  return filtered.slice(0, limit).map((v) => ({
    id: v.videoId,
    title: v.title,
    artist: v.author?.name || "Unknown",
    album: undefined,
    artwork: `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`,
    duration: v.duration?.seconds || 0,
    explicit: false,
    source: "youtube" as const,
    permalinkUrl: `https://youtube.com/watch?v=${v.videoId}`,
  }));
}

async function pickYoutubeMatch(
  artist: string,
  title: string,
  durationSec: number | null = null,
): Promise<string | null> {
  const { default: yts } = await import("yt-search");
  // "Topic" auto-uploads are indexed under the plain "artist - title" query;
  // adding "audio" nudges results toward the label's own upload if there is
  // no Topic channel.
  const q = `${artist} ${title} audio`;
  const raw = (await (yts as unknown as (q: string) => Promise<{
    videos?: YtVideoLike[];
  }>)(q)).videos ?? [];
  const scored = raw
    .map((v) => ({ v, s: scoreYtVideo(v, artist, title, durationSec) }))
    .filter((x) => x.s > 0)
    .sort((a, b) => b.s - a.s);
  return scored[0]?.v.videoId ?? null;
}

// ISO 8601 duration as used by DASH: PT29.907S, PT3M42.5S, PT1H2M3S.
function parseIsoDuration(v: string): number | null {
  const m = /^PT(?:([\d.]+)H)?(?:([\d.]+)M)?(?:([\d.]+)S)?$/.exec(v.trim());
  if (!m) return null;
  const [, h, min, s] = m;
  const total =
    (h ? parseFloat(h) * 3600 : 0) +
    (min ? parseFloat(min) * 60 : 0) +
    (s ? parseFloat(s) : 0);
  return Number.isFinite(total) && total > 0 ? total : null;
}

// A source can hand back something that resolves fine and still won't play:
// Tidal serves a 30-second preview when the session isn't entitled to the
// track, and any upstream can go down between resolving and streaming. Both
// used to count as success, so the chain stopped at the first source that
// answered and the player was left spinning on a stream that never arrived.
// Verifying here is what lets a bad source fall through to the next one.
const PLAYABILITY_TIMEOUT_MS = 6000;

// The yt-dlp backend downloads the whole track before it serves a byte, so its
// first request for a given track takes as long as the download does. Probing
// it on the 6s budget would reject it every time on a cold cache and accept it
// instantly afterwards; give it a window wide enough to actually finish.
const SCDLP_PLAYABILITY_TIMEOUT_MS =
  Number(process.env.SC_DLP_PLAYABILITY_TIMEOUT_MS) || 25000;

async function assertPlayable(
  result: StreamResult,
  meta?: StreamMeta,
): Promise<void> {
  if (result.rawDash) {
    const declared = /mediaPresentationDuration="([^"]+)"/.exec(result.rawDash);
    const seconds = declared ? parseIsoDuration(declared[1]) : null;
    // Only call it a preview when we know the real length and the manifest is
    // dramatically shorter, so genuinely short tracks aren't rejected.
    if (seconds !== null && meta?.duration && meta.duration > 60) {
      if (seconds < meta.duration * 0.5) {
        throw new Error(
          `preview only (${Math.round(seconds)}s of ${Math.round(meta.duration)}s)`,
        );
      }
    }
    return;
  }

  // Direct URL: confirm bytes are actually served before committing to it.
  const ctrl = new AbortController();
  const timer = setTimeout(
    () => ctrl.abort(),
    result.source === "scdlp"
      ? SCDLP_PLAYABILITY_TIMEOUT_MS
      : PLAYABILITY_TIMEOUT_MS,
  );
  try {
    const probe = await fetch(result.url, {
      headers: { ...streamFetchHeaders(result.url, {}), range: "bytes=0-1" },
      signal: ctrl.signal,
    });
    if (!probe.ok && probe.status !== 206) {
      // A muted signing token is the one octave failure worth paying attention
      // to: the probe retried every 6s since is useless, so drop the cached
      // token now and let the next play mint a fresh one.
      if (result.source === "octave" && (probe.status === 401 || probe.status === 403)) {
        invalidateOctaveToken();
      }
      throw new Error(`upstream returned ${probe.status}`);
    }
    await probe.body?.cancel();
  } finally {
    clearTimeout(timer);
  }
}

async function resolveStreamWithFallback(
  id: string,
  isrc?: string,
  quality?: string,
  sourceHint?: SourcePriority,
  meta?: StreamMeta,
  octaveToken?: string,
): Promise<StreamResult> {
  const started = Date.now();
  const attempt = async (s: SourcePriority) => {
    try {
      const result = await resolveFromSource(
        s,
        id,
        isrc,
        quality,
        sourceHint,
        meta,
        octaveToken,
      );
      await assertPlayable(result, meta);
      console.log(`[stream] ${s} ready in ${Date.now() - started}ms`);
      return result;
    } catch (e) {
      console.error(`${s} stream failed:`, e instanceof Error ? e.message : e);
      throw e;
    }
  };

  // Every source is raced at once and the first one to produce a *verified
  // playable* stream wins — no quality tiers, no sequential fallback. A source
  // that needs an ISRC it wasn't given rejects immediately and costs nothing.
  const isOctavePick = sourceHint === "octave";
  const candidateAllowed = (s: SourcePriority): boolean => {
    if ((s === "qobuz" || s === "deezer") && !isrc) return false;
    if (s === "scdlp" && SCDLP_BASES.length === 0) return false;
    // Costs a round trip to learn what we already know locally: without a
    // session it can only 401, and it can't match without a title+artist.
    if (s === "monochrome" && !monochromeIsAuthed()) return false;
    if (s === "monochrome" && !(meta?.title && meta?.artist)) return false;
    // Octave mirrors Tidal's stream ids — without a Tidal context it can only
    // 404 (and may waste a browser launch fetching a token for no reason).
    if (s === "octave" && sourceHint !== "tidal" && sourceHint !== "octave")
      return false;
    // Picking Octave in the UI means the stream *should* come from Octave, so
    // this pass races octave alone — the fallback below re-races everything
    // else only when octave outright fails.
    if (isOctavePick && s !== "octave") return false;
    return true;
  };

  const candidates = SOURCE_ORDER.filter(candidateAllowed);

  if (candidates.length === 0) throw new Error("No usable stream sources");

  try {
    return await Promise.any(candidates.map(attempt));
  } catch (e) {
    // A UI octave pick raced nothing but octave; if that failed (dead token,
    // missing token, upstream down), re-race the rest so the play survives.
    if (isOctavePick) {
      const rest = SOURCE_ORDER.filter(
        (s) => s !== "octave" && candidateAllowed(s),
      );
      if (rest.length > 0) {
        try {
          return await Promise.any(rest.map(attempt));
        } catch (fallbackErr) {
          const detail =
            fallbackErr instanceof AggregateError
              ? fallbackErr.errors
                  .map((err, i) =>
                    `${rest[i]}: ${err instanceof Error ? err.message : err}`,
                  )
                  .join(" | ")
              : String(fallbackErr);
          throw new Error(`All stream sources failed (octave then fallback) — ${detail}`);
        }
      }
    }
    const detail =
      e instanceof AggregateError
        ? e.errors
            .map((err, i) =>
              `${candidates[i]}: ${err instanceof Error ? err.message : err}`,
            )
            .join(" | ")
        : String(e);
    throw new Error(`All stream sources failed — ${detail}`);
  }
}

const DEEZER_QUALITY_FORMATS: Record<string, string> = {
  HI_RES_LOSSLESS: "FLAC",
  LOSSLESS: "FLAC",
  HIGH: "MP3_320",
  LOW: "MP3_128",
  NORMAL: "MP3_128",
};

async function resolveDeezerStreamUrl(
  isrc: string,
  quality?: string,
): Promise<{ url: string; mimeType: string }> {
  const format = DEEZER_QUALITY_FORMATS[quality ?? ""] ?? "FLAC";
  const mimeType = format.startsWith("MP3") ? "audio/mpeg" : "audio/flac";
  let lastErr: unknown;

  for (const base of DEEZER_BASES) {
    const url = `${base.replace(/\/+$/, "")}/stream/?isrc=${encodeURIComponent(
      isrc,
    )}&format=${format}`;
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), DEEZER_TIMEOUT_MS);
      let res: Response;
      try {
        res = await fetch(url, {
          headers: streamFetchHeaders(url, { Range: "bytes=0-1" }),
          signal: ctrl.signal,
        });
      } catch {
        clearTimeout(t);
        return { url, mimeType };
      }
      clearTimeout(t);
      res.body?.cancel().catch(() => {});
      if (res.ok || res.status === 206) return { url, mimeType };
      if (res.status === 404) {
        lastErr = new Error("Deezer 404 (no match)");
        continue;
      }
      lastErr = new Error(`Deezer HTTP ${res.status}`);
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr ?? new Error("No Deezer base resolved this ISRC");
}

async function resolveSoundcloudStreamUrl(
  trackId: number,
): Promise<{ url: string; mimeType: string }> {
  const track = await soundcloudApiRequest<SoundcloudTrackInfo>(
    `/tracks/${trackId}`,
  );

  const clientId = await getValidClientId();

  const transcodings = track.media?.transcodings || [];
  const progressive = transcodings.find(
    (t) => t.format?.protocol === "progressive",
  );
  const hls = transcodings.find((t) => t.format?.protocol === "hls");
  const transcoding = progressive || hls;

  if (transcoding) {
    const resolveUrl = new URL(transcoding.url);
    resolveUrl.searchParams.set("client_id", clientId);
    const ctrl = new AbortController();
    const timeout = setTimeout(() => ctrl.abort(), MUSIC_TIMEOUT_MS);
    const res = await fetch(resolveUrl.toString(), {
      signal: ctrl.signal,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        Accept: "application/json",
        Origin: "https://soundcloud.com",
        Referer: "https://soundcloud.com/",
      },
    });
    clearTimeout(timeout);
    if (!res.ok) {
      discard(res);
      throw new Error(`SoundCloud transcoding resolve failed: ${res.status}`);
    }
    const data = (await res.json()) as { url: string };
    if (!data.url) throw new Error("No URL in SoundCloud transcoding response");
    const mimeType = progressive
      ? "audio/mpeg"
      : "application/vnd.apple.mpegurl";
    return { url: data.url, mimeType };
  }

  if (track.stream_url) {
    const streamUrl = new URL(track.stream_url);
    streamUrl.searchParams.set("client_id", clientId);
    return { url: streamUrl.toString(), mimeType: "audio/mpeg" };
  }

  throw new Error("No stream available for this SoundCloud track");
}

// --- yt-dlp SoundCloud backend (soundcloud-backend/) ------------------------

interface ScDlpTrack {
  url: string;
  title: string;
  artist: string;
  artistUrl?: string;
  duration: number;
  thumb?: string;
  src?: string;
}

const SCDLP_TIMEOUT_MS = Number(process.env.SC_DLP_TIMEOUT_MS) || 8000;

/** The backend streams search hits as SSE so the UI can render them as they
 * land. We only want the finished list, so collect until [DONE] or timeout. */
async function scdlpSse(url: string, timeoutMs: number): Promise<ScDlpTrack[]> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    if (!res.ok) {
      discard(res);
      throw new Error(`scdlp HTTP ${res.status}`);
    }
    const body = await res.text();
    const out: ScDlpTrack[] = [];
    for (const line of body.split("\n")) {
      if (!line.startsWith("data: ")) continue;
      const payload = line.slice(6).trim();
      if (!payload || payload === "[DONE]") continue;
      try {
        const t = JSON.parse(payload) as ScDlpTrack;
        if (t?.url) out.push(t);
      } catch {}
    }
    return out;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeForMatch(s: string): string {
  return s
    .toLowerCase()
    .replace(/\([^)]*\)|\[[^\]]*\]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/** Pick the search hit that actually corresponds to the requested track.
 * SoundCloud search is full of remixes, slowed edits and DJ sets, and playing
 * one of those instead of the song is worse than the source failing. */
function pickScDlpMatch(
  results: ScDlpTrack[],
  artist: string,
  title: string,
  durationSec?: number,
): ScDlpTrack | null {
  const wantTitle = normalizeForMatch(title);
  const wantArtist = normalizeForMatch(artist);
  let best: { t: ScDlpTrack; score: number } | null = null;

  for (const t of results) {
    const gotTitle = normalizeForMatch(t.title || "");
    const gotArtist = normalizeForMatch(t.artist || "");
    let score = 0;

    if (gotTitle.includes(wantTitle)) score += 40;
    else continue; // wrong song entirely

    if (gotArtist.includes(wantArtist) || gotTitle.includes(wantArtist))
      score += 30;
    if (YT_JUNK_TERMS.some((k) => (t.title || "").toLowerCase().includes(k)))
      score -= 60;

    if (durationSec && t.duration) {
      const diff = Math.abs(t.duration - durationSec);
      if (diff <= 3) score += 30;
      else if (diff <= 8) score += 15;
      else if (diff > 30) score -= 40;
    }

    if (score > 0 && (!best || score > best.score)) best = { t, score };
  }

  return best?.t ?? null;
}

async function scdlpSearch(
  query: string,
  limit: number,
): Promise<ClientTrack[]> {
  let lastErr: unknown;
  for (const base of SCDLP_BASES) {
    const url = `${base.replace(/\/+$/, "")}/api/music/search?q=${encodeURIComponent(
      query,
    )}&limit=${Math.min(limit, 40)}`;
    try {
      const hits = await scdlpSse(url, SCDLP_TIMEOUT_MS);
      if (hits.length === 0) continue;
      return hits.map((t) => ({
        id: t.url,
        title: t.title || "Unknown",
        artist: t.artist || "Unknown",
        artwork: t.thumb || "",
        duration: t.duration || 0,
        explicit: false,
        source: "scdlp" as const,
        permalinkUrl: t.url,
      }));
    } catch (e) {
      lastErr = e;
    }
  }
  if (lastErr) throw lastErr;
  return [];
}

/** Resolve to the backend's own range-capable audio endpoint. The service
 * downloads and disk-caches the track, so the URL is stable and seekable —
 * our /api/music/stream proxy can pipe it straight through. */
async function resolveScDlpStreamUrl(
  id: string,
  sourceHint: SourcePriority | undefined,
  meta: StreamMeta | undefined,
): Promise<{ url: string; mimeType: string }> {
  if (SCDLP_BASES.length === 0) throw new Error("No scdlp base configured");

  const direct = /^https?:\/\/(www\.)?soundcloud\.com\//i.test(id) ? id : null;

  let lastErr: unknown;
  for (const base of SCDLP_BASES) {
    const root = base.replace(/\/+$/, "");
    try {
      let permalink = direct;

      if (!permalink) {
        if (!meta?.artist || !meta?.title) {
          throw new Error("No SoundCloud URL or artist/title to search with");
        }
        const hits = await scdlpSse(
          `${root}/api/music/search?q=${encodeURIComponent(
            `${meta.artist} ${meta.title}`,
          )}&limit=10`,
          SCDLP_TIMEOUT_MS,
        );
        const match = pickScDlpMatch(
          hits,
          meta.artist,
          meta.title,
          meta.duration,
        );
        if (!match) throw new Error("No scdlp match for this track");
        permalink = match.url;
      }

      // Kick the download off now so the cache is filling while the race is
      // still running; the playability probe below is what decides whether
      // this source is actually ready to serve.
      const streamUrl = `${root}/api/sc/stream?url=${encodeURIComponent(permalink)}`;
      fetch(`${root}/api/music/prewarm`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: permalink }),
      }).catch(() => {});

      return { url: streamUrl, mimeType: "audio/mpeg" };
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr ?? new Error("All scdlp bases failed");
}

async function resolveTidalStreamUrl(
  id: string,
  quality: string,
): Promise<{
  url: string;
  codec?: string;
  mimeType?: string;
  isDash?: boolean;
  rawDash?: string;
}> {
  const data = await callMusicApi<any>(
    "/trackManifests/",
    { id, quality, adaptive: "false", formats: "FLAC" },
    MUSIC_STREAM_BASES,
  );

  const uri = data?.data?.data?.attributes?.uri;
  if (!uri) throw new Error("No manifest URI in response");

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), MUSIC_TIMEOUT_MS);
  const mpdRes = await fetch(uri, { signal: ctrl.signal });
  clearTimeout(t);

  if (!mpdRes.ok) throw new Error(`MPD fetch failed: ${mpdRes.status}`);
  const rawDash = await mpdRes.text();

  return {
    url: uri,
    isDash: true,
    rawDash,
    mimeType: "application/dash+xml",
  };
}

// youtube-dl-exec defaults to a yt-dlp it downloads into its own node_modules
// during postinstall. That download is skipped by --frozen-lockfile installs,
// CI caches and offline installs, leaving the package pointed at a binary that
// isn't there — every YouTube resolve then dies on ENOENT, which reads as
// "the fallback doesn't work" rather than as a missing dependency. Prefer an
// explicitly configured path, then the bundled one, then whatever yt-dlp is on
// PATH.
let ytDlpPathPromise: Promise<string | null> | null = null;
function resolveYtDlpPath(): Promise<string | null> {
  if (!ytDlpPathPromise) {
    ytDlpPathPromise = (async () => {
      const mod = (await import("youtube-dl-exec")) as unknown as {
        constants?: { YOUTUBE_DL_PATH?: string };
      };
      const candidates = [
        process.env.YT_DLP_PATH,
        mod.constants?.YOUTUBE_DL_PATH,
        "/usr/local/bin/yt-dlp",
        "/usr/bin/yt-dlp",
      ].filter((p): p is string => !!p);
      for (const candidate of candidates) {
        try {
          await fsp.access(candidate, fsConstants.X_OK);
          return candidate;
        } catch {
          // try the next one
        }
      }
      return null;
    })();
  }
  return ytDlpPathPromise;
}

// yt-dlp can sit for minutes when YouTube throws a bot check at it. Without a
// bound the request never settles and the player just spins, so cap it and let
// the caller move on to the next source.
const YT_DLP_TIMEOUT_MS = Number(process.env.YT_DLP_TIMEOUT_MS) || 20000;

async function resolveYoutubeStreamUrl(
  videoId: string,
): Promise<{ url: string; mimeType: string }> {
  const binaryPath = await resolveYtDlpPath();
  if (!binaryPath) {
    throw new Error(
      "yt-dlp not found. Install it, or set YT_DLP_PATH to its location.",
    );
  }
  const { create } = await import("youtube-dl-exec");
  const youtubeDl = create(binaryPath);
  const videoUrl = `https://www.youtube.com/watch?v=${videoId}`;
  // Third argument is execa's options, not yt-dlp's: anything in the second
  // object is turned into a CLI flag, so a timeout there becomes --timeout and
  // yt-dlp exits on "no such option".
  const info = (await youtubeDl(
    videoUrl,
    { format: "bestaudio", getUrl: true },
    { timeout: YT_DLP_TIMEOUT_MS },
  )) as unknown as string;
  const url = String(info).trim().split("\n")[0];
  if (!url.startsWith("http")) {
    throw new Error("yt-dlp returned no stream URL");
  }
  return {
    url,
    mimeType: "audio/mp4",
  };
}

export async function musicRoutes(fastify: FastifyInstance) {
  fastify.get("/api/music/source", async (req, reply) => {
    schedulePing();
    const latenciesObj: Record<string, number> = {};
    for (const [k, v] of sourceLatencies.entries()) {
      if (v !== Infinity) {
        latenciesObj[k] = v;
      }
    }
    return reply.send({
      current: currentSourcePriority,
      latencies: latenciesObj,
      lastPing: lastPingTime,
    });
  });

  fastify.post("/api/music/source", async (req, reply) => {
    const { source } = req.body as { source?: SourcePriority };
    // Stream-only sources normally can't be picked (they have no search of
    // their own). Octave is the exception: selecting it in the UI means "stream
    // this through octave", so it is allowed here and search still backfills
    // from Tidal (its ids are Tidal ids anyway).
    const selectable = SOURCE_ORDER.filter(
      (s) => s === "octave" || !STREAM_ONLY_SOURCES.has(s),
    );
    if (source && selectable.includes(source)) {
      currentSourcePriority = source;
      return reply.send({
        current: currentSourcePriority,
        message: `Switched to ${source}`,
      });
    }
    return musicError(
      reply,
      400,
      `Invalid source. Use one of: ${selectable.join(", ")}`,
    );
  });

  // Monochrome's resolver gates /playback behind Cloudflare Turnstile, which
  // only a real browser can satisfy. A human opens this page once and the
  // resulting session is cached server-side and reused until it expires.
  fastify.get("/api/music/monochrome/auth", async (_req, reply) => {
    const cfg = await monochromeGetConfig();
    return reply.type("text/html").send(MONOCHROME_AUTH_PAGE(cfg.turnstile_site_key));
  });

  fastify.post("/api/music/monochrome/turnstile", async (req, reply) => {
    const token = (req.body as { turnstile_token?: string })?.turnstile_token;
    if (!token)
      return reply.code(400).send({ ok: false, error: "missing turnstile_token" });
    try {
      const exp = await monochromeExchangeTurnstile(token);
      return reply.send({ ok: true, expiresAt: exp });
    } catch (e) {
      return reply
        .code(502)
        .send({ ok: false, error: e instanceof Error ? e.message : "failed" });
    }
  });

  // Carries the site key so the client can render the widget off one call
  // rather than round-tripping to the upstream /config itself.
  fastify.get("/api/music/monochrome/status", async (_req, reply) => {
    const state = monochromeAuthState();
    let siteKey: string | null = null;
    let action = "auth";
    let enabled = true;
    if (!state.authenticated) {
      try {
        const cfg = await monochromeGetConfig();
        siteKey = cfg.turnstile_site_key;
        action = cfg.turnstile_action || "auth";
        enabled = cfg.turnstile_enabled !== false;
      } catch {
        // Leaving siteKey null makes the client skip silently instead of
        // rendering a widget that can't possibly validate.
      }
    }
    return reply.send({ ...state, siteKey, action, turnstileEnabled: enabled });
  });

  // Octave mints its playback token by driving a real browser
  // (scripts/get-pbtoken.mjs) — there is no HTTP endpoint that issues it.
  // Status lets an operator see whether the token pipeline is warm before
  // blaming an octave win/loss on the stream itself.
  fastify.get("/api/music/octave/status", async (_req, reply) => {
    return reply.send({
      ...octaveStatus(),
      script: octaveScriptPath(),
    });
  });

  fastify.post("/api/music/octave/refresh", async (_req, reply) => {
    const token = await refreshOctaveToken();
    return reply.send({
      ...octaveStatus(),
      refreshed: !!token,
      script: octaveScriptPath(),
    });
  });

  fastify.post("/api/music/ping", async (req, reply) => {
    schedulePing();
    return reply.send({
      current: currentSourcePriority,
      latencies: Object.fromEntries(sourceLatencies),
    });
  });

  fastify.get("/api/music/search", async (req, reply) => {
    const { q, limit, source } = req.query as {
      q?: string;
      limit?: string;
      source?: SourcePriority;
    };
    if (!q?.trim()) return reply.send({ items: [], albums: [], artists: [] });

    schedulePing();
    const sourceToUse = source || currentSourcePriority;
    const lim = parseInt(limit ?? "30");

    if (sourceToUse === "tidal") {
      try {
        let trackItems: TidalTrack[] = [];

        let albumItems: any[] = [];

        let artistItems: any[] = [];

        const combined = await callMusicApi<unknown>("/search/", {
          q: q.trim(),
          limit: lim,
        }).catch(() => null);
        if (combined) {
          trackItems = extractSearchSection(combined, "tracks") as TidalTrack[];
          albumItems = extractSearchSection(combined, "albums");
          artistItems = extractSearchSection(combined, "artists");
        }

        if (trackItems.length === 0) {
          const [tracksResp, albumsResp, artistsResp] =
            await Promise.allSettled([
              callMusicApi<unknown>("/search/", { s: q.trim(), limit: lim }),
              callMusicApi<unknown>("/search/", {
                al: q.trim(),
                limit: Math.min(lim, 20),
              }),
              callMusicApi<unknown>("/search/", {
                a: q.trim(),
                limit: Math.min(lim, 12),
              }),
            ]);

          if (tracksResp.status === "fulfilled") {
            const raw: any =
              (tracksResp.value as any)?.data ?? tracksResp.value;
            if (Array.isArray(raw?.items)) trackItems = raw.items;
          }
          if (albumItems.length === 0 && albumsResp.status === "fulfilled") {
            albumItems = extractSearchSection(albumsResp.value, "albums");
          }
          if (artistItems.length === 0 && artistsResp.status === "fulfilled") {
            const raw: any =
              (artistsResp.value as any)?.data ?? artistsResp.value;
            artistItems = Array.isArray(raw?.items)
              ? raw.items
              : extractSearchSection(artistsResp.value, "artists");
          }
        }

        const tracks: ClientTrack[] = trackItems
          .map((t) => toClientTrack(t))
          .slice(0, lim);

        const albums: ClientAlbum[] = albumItems
          .map((a: any) => toClientAlbum(a))
          .slice(0, 12);

        const artists: ClientArtist[] = artistItems
          .map((a: any) => toClientArtist(a))
          .slice(0, 12);

        if (tracks.length > 0 || albums.length > 0 || artists.length > 0) {
          reply.header("cache-control", "public, max-age=120");
          return reply.send({
            items: tracks,
            albums,
            artists,
            source: "tidal",
          });
        }
      } catch {}
    }

    const items = await searchWithPriority(q, lim, source);
    reply.header("cache-control", "public, max-age=120");
    return reply.send({
      items,
      albums: [],
      artists: [],
      source: currentSourcePriority,
    });
  });

  fastify.get("/api/music/album/:id", async (req, reply) => {
    const { id } = req.params as { id: string };
    try {
      const data = await callMusicApi<unknown>("/album/", { id });

      const raw: any = (data as any)?.data ?? data;

      let albumRaw: any = null;

      let trackItems: any[] = [];

      if (raw && typeof raw === "object" && !Array.isArray(raw)) {
        if ("numberOfTracks" in raw || "title" in raw) {
          albumRaw = raw;
        }
        if (Array.isArray(raw.items)) {
          trackItems = raw.items;
          if (!albumRaw && trackItems.length > 0) {
            const first = trackItems[0].item || trackItems[0];
            if (first?.album) albumRaw = first.album;
          }
        }
      }

      if (!albumRaw) {
        return musicError(reply, 404, "Album not found");
      }

      const album = toClientAlbum(albumRaw);
      const tracks: ClientTrack[] = trackItems

        .map((i: any) => toClientTrack(i.item || i))
        .filter((t) => t.id);

      reply.header("cache-control", "public, max-age=300");
      return reply.send({ album, tracks });
    } catch (error) {
      return musicError(reply, 502, "Failed to fetch album", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  });

  fastify.get("/api/music/artist/:id", async (req, reply) => {
    const { id } = req.params as { id: string };
    try {
      const data = await callMusicApi<unknown>("/artist/", { id });

      const raw: any = (data as any)?.data ?? data;

      const artistRaw =
        raw?.artist ||
        (Array.isArray(raw) ? raw[0] : null) ||
        (raw?.id ? raw : null);
      if (!artistRaw) return musicError(reply, 404, "Artist not found");

      const artist = toClientArtist(artistRaw);

      const albumMap = new Map<number, ClientAlbum>();
      const trackMap = new Map<number, ClientTrack>();

      const scan = (value: any, visited: Set<unknown>) => {
        if (!value || typeof value !== "object" || visited.has(value)) return;
        visited.add(value);
        if (Array.isArray(value)) {
          value.forEach((item) => scan(item, visited));
          return;
        }
        const item = value.item || value;
        const hasAlbumFields =
          "numberOfTracks" in item || "numberOfItems" in item;
        const hasTrackFields = item.duration && item.trackNumber != null;
        if (hasAlbumFields && item.id)
          albumMap.set(item.id, toClientAlbum(item));
        else if (hasTrackFields && item.id)
          trackMap.set(item.id, toClientTrack(item));
        Object.values(value).forEach((nested) => scan(nested, visited));
      };

      scan(raw, new Set());

      if (albumMap.size === 0) {
        try {
          const albumsData = await callMusicApi<unknown>("/artist/", {
            f: id,
            skip_tracks: "true",
          });
          scan((albumsData as any)?.data ?? albumsData, new Set());
        } catch {}
      }

      const allAlbums = Array.from(albumMap.values()).sort((a, b) =>
        (b.releaseDate || "").localeCompare(a.releaseDate || ""),
      );
      const eps = allAlbums.filter(
        (a) => a.type === "EP" || a.type === "SINGLE",
      );
      const albums = allAlbums.filter((a) => !eps.includes(a));
      const tracks = Array.from(trackMap.values()).slice(0, 15);

      reply.header("cache-control", "public, max-age=300");
      return reply.send({ artist, albums, eps, tracks });
    } catch (error) {
      return musicError(reply, 502, "Failed to fetch artist", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  });

  fastify.get("/api/music/artist/:id/similar", async (req, reply) => {
    const { id } = req.params as { id: string };
    try {
      const data = await callMusicApi<any>("/artist/similar/", { id });

      const artists: ClientArtist[] = (
        data?.artists ||
        data?.data?.artists ||
        []
      ).map((a: any) => toClientArtist(a));
      reply.header("cache-control", "public, max-age=600");
      return reply.send({ artists });
    } catch (error) {
      return reply.send({ artists: [] });
    }
  });

  fastify.get("/api/music/album/:id/similar", async (req, reply) => {
    const { id } = req.params as { id: string };
    try {
      const data = await callMusicApi<any>("/album/similar/", { id });

      const raw = data?.data ?? data;

      const items = Array.isArray(raw?.items)
        ? raw.items
        : Array.isArray(raw)
          ? raw
          : [];

      const albums: ClientAlbum[] = items.map((a: any) => toClientAlbum(a));
      reply.header("cache-control", "public, max-age=600");
      return reply.send({ albums });
    } catch (error) {
      return reply.send({ albums: [] });
    }
  });

  // DASH segment relay. The upstream host is pinned to AUDIO_PROXY_BASE, so
  // the wildcard can't be aimed at internal services — the only thing the
  // caller controls is the path handed to a proxy that is already publicly
  // reachable on its own.
  fastify.get(`${SEGMENT_PREFIX}*`, async (req, reply) => {
    const raw = (req.params as Record<string, string>)["*"] ?? "";
    let target: string;
    try {
      target = decodeURIComponent(raw);
    } catch {
      return musicError(reply, 400, "Malformed segment URL");
    }
    if (!/^https:\/\//i.test(target)) {
      return musicError(reply, 400, "Segment URL must be https");
    }

    const headers: Record<string, string> = {};
    const range = req.headers.range;
    if (typeof range === "string") headers["range"] = range;

    // Tie the upstream fetch to the client connection. Without this, a client
    // that goes away mid-stream (skip, seek, closed tab) leaves the upstream
    // pulling audio into a buffer nobody will ever read.
    const abort = new AbortController();
    req.raw.on("close", () => abort.abort());

    let upstream: Response;
    try {
      upstream = await fetch(`${AUDIO_PROXY_BASE}/${target}`, {
        headers,
        signal: abort.signal,
        // Bun honours this per-request; it does not leak to other fetches.
        ...(AUDIO_PROXY_INSECURE
          ? { tls: { rejectUnauthorized: false } }
          : {}),
      } as RequestInit);
    } catch (e) {
      return musicError(reply, 502, "Segment fetch failed", {
        detail: e instanceof Error ? e.message : String(e),
      });
    }

    reply.code(upstream.status);
    for (const h of [
      "content-type",
      "content-length",
      "content-range",
      "accept-ranges",
    ]) {
      const v = upstream.headers.get(h);
      if (v) reply.header(h, v);
    }
    reply.header("cache-control", "private, max-age=3600");
    reply.header("access-control-allow-origin", "*");
    if (!upstream.body) return reply.send();
    // Counted so /api/system/memory can show whether streams are actually
    // being torn down. A rising `open` that never falls means buffered audio
    // is accumulating in stream plumbing the JS heap never sees.
    streamProxy.open++;
    streamProxy.started++;
    let settled = false;
    const done = () => {
      if (settled) return;
      settled = true;
      streamProxy.open--;
      streamProxy.finished++;
    };
    const node = Readable.fromWeb(upstream.body as unknown as NodeReadableStream);
    node.on("close", done);
    node.on("error", done);
    reply.raw.on("close", done);
    return reply.send(node);
  });

  fastify.get("/api/music/stream", async (req, reply) => {
    const { id, quality, isrc, source, title, artist, duration, k } = req.query as {
      id?: string;
      quality?: string;
      isrc?: string;
      source?: SourcePriority;
      title?: string;
      artist?: string;
      duration?: string;
      k?: string;
    };
    if (!id) return musicError(reply, 400, "Missing required parameter: id");

    schedulePing();
    const sourceToUse = source || currentSourcePriority;

    let streamUrl: string;
    let streamMime: string;

    try {
      const result = await resolveStreamWithFallback(
        id,
        isrc,
        quality,
        sourceToUse,
        { title, artist, duration: duration ? Number(duration) : undefined },
        k,
      );
      streamUrl = result.url;
      streamMime = result.mimeType;
    } catch (error) {
      return musicError(reply, 502, "All stream sources failed", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }

    const headers: Record<string, string> = {};
    const range = req.headers.range;
    if (typeof range === "string") headers["range"] = range;

    // Same as the segment proxy: without binding the upstream to the client
    // connection, every skip and every seek orphans a live audio stream that
    // keeps buffering into memory. Seeking is the worst case, because each
    // seek opens a fresh range request and abandons the previous one.
    const abort = new AbortController();
    req.raw.on("close", () => abort.abort());

    let upstream: Response;
    try {
      upstream = await fetch(streamUrl, {
        headers: streamFetchHeaders(streamUrl, headers),
        signal: abort.signal,
      });
    } catch (e) {
      return musicError(reply, 502, "Upstream fetch failed", {
        detail: e instanceof Error ? e.message : String(e),
      });
    }

    reply.code(upstream.status);
    reply.header(
      "content-type",
      upstream.headers.get("content-type") || streamMime || "audio/mp4",
    );
    const contentLength = upstream.headers.get("content-length");
    if (contentLength) reply.header("content-length", contentLength);
    const contentRange = upstream.headers.get("content-range");
    if (contentRange) reply.header("content-range", contentRange);
    reply.header(
      "accept-ranges",
      upstream.headers.get("accept-ranges") || "bytes",
    );
    reply.header("cache-control", "no-store");

    if (!upstream.body) return reply.send();
    // Counted so /api/system/memory can show whether streams are actually
    // being torn down. A rising `open` that never falls means buffered audio
    // is accumulating in stream plumbing the JS heap never sees.
    streamProxy.open++;
    streamProxy.started++;
    let settled = false;
    const done = () => {
      if (settled) return;
      settled = true;
      streamProxy.open--;
      streamProxy.finished++;
    };
    const node = Readable.fromWeb(upstream.body as unknown as NodeReadableStream);
    node.on("close", done);
    node.on("error", done);
    reply.raw.on("close", done);
    return reply.send(node);
  });

  fastify.get("/api/music/track", async (req, reply) => {
    const { id, quality, isrc, source, title, artist, duration, k } = req.query as {
      id?: string;
      quality?: string;
      isrc?: string;
      source?: SourcePriority;
      title?: string;
      artist?: string;
      duration?: string;
      k?: string;
    };
    if (!id) return musicError(reply, 400, "Missing required parameter: id");

    schedulePing();
    const sourceToUse = source || currentSourcePriority;

    try {
      const result = await resolveStreamWithFallback(
        id,
        isrc,
        quality,
        sourceToUse,
        { title, artist, duration: duration ? Number(duration) : undefined },
        k,
      );
      if (result.source === "octave") result.url = redactOctaveToken(result.url);
      return reply.send(result);
    } catch (error) {
      return musicError(reply, 502, "All stream sources failed", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  });

  fastify.get("/api/music/manifest", async (req, reply) => {
    const { id, quality, isrc, artist, title, source, duration, k } =
      req.query as {
        id?: string;
        quality?: string;
        isrc?: string;
        artist?: string;
        title?: string;
        source?: SourcePriority;
        duration?: string;
        k?: string;
      };

    if (!id) return musicError(reply, 400, "Missing required parameter: id");

    schedulePing();
    const sourceToUse = source || currentSourcePriority;

    if (isrc) {
      try {
        const d = await resolveDeezerStreamUrl(isrc, quality);
        reply.header("cache-control", "no-store");
        return reply.send({
          url: d.url,
          mimeType: d.mimeType,
          source: "deezer",
        });
      } catch {}
    }

    try {
      const result = await resolveStreamWithFallback(
        id,
        isrc,
        quality,
        sourceToUse,
        { title, artist, duration: duration ? Number(duration) : undefined },
        k,
      );
      if (result.source === "octave") result.url = redactOctaveToken(result.url);

      if (result.source === "tidal" && (result.isDash || result.rawDash)) {
        let rawDash = result.rawDash;
        if (!rawDash) {
          const mpdRes = await fetch(result.url);
          rawDash = await mpdRes.text();
        }
        // Absolute, not relative: the player loads this MPD from a blob URL
        // when clearkey DRM is in play, and relative segment paths would
        // resolve against blob: and 404.
        const self = `${req.protocol}://${req.headers.host}`;
        const rewritten = rawDash.replace(
          /(initialization|media)="(https:\/\/[^"]+)"/g,
          (_, attr, url) =>
            `${attr}="${self}${SEGMENT_PREFIX}${encodeURIComponent(url)}"`,
        );
        reply.header("content-type", "application/dash+xml");
        reply.header("cache-control", "private, max-age=300");
        reply.header("access-control-allow-origin", "*");
        return reply.send(rewritten);
      }

      reply.header("cache-control", "private, max-age=300");
      reply.header("access-control-allow-origin", "*");
      return reply.send(result);
    } catch (error) {
      return musicError(reply, 502, "All stream sources failed", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  });

  fastify.get("/api/music/soundcloud/track/:id", async (req, reply) => {
    const { id } = req.params as { id: string };
    try {
      const track = await soundcloudApiRequest<SoundcloudTrackInfo>(
        `/tracks/${id}`,
      );
      reply.header("cache-control", "public, max-age=300");
      return reply.send(toSoundcloudClientTrack(track as SoundcloudTrack));
    } catch (error) {
      return musicError(reply, 404, "Track not found", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  });

  fastify.get("/api/music/soundcloud/playlist/:id", async (req, reply) => {
    const { id } = req.params as { id: string };
    try {
      const playlist = await soundcloudApiRequest<SoundcloudPlaylist>(
        `/playlists/${id}`,
      );
      const tracks = (playlist.tracks || []).map(toSoundcloudClientTrack);
      reply.header("cache-control", "public, max-age=300");
      return reply.send({
        id: playlist.id,
        title: playlist.title,
        artist: playlist.user.username,
        trackCount: playlist.track_count,
        tracks,
        artwork: soundcloudArtworkUrl(playlist.artwork_url),
        permalinkUrl: playlist.permalink_url,
        description: playlist.description,
      });
    } catch (error) {
      return musicError(reply, 404, "Playlist not found", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  });

  fastify.get(
    "/api/music/soundcloud/user/:username/tracks",
    async (req, reply) => {
      const { username } = req.params as { username: string };
      const { limit } = req.query as { limit?: string };
      try {
        const tracks = await soundcloudApiRequest<SoundcloudSearchResp>(
          `/users/${username}/tracks`,
          {
            limit: limit ?? 50,
          },
        );
        const items = (tracks?.collection || []).map(toSoundcloudClientTrack);
        reply.header("cache-control", "public, max-age=300");
        return reply.send({ username, items });
      } catch (error) {
        return musicError(reply, 404, "User tracks not found", {
          detail: error instanceof Error ? error.message : String(error),
        });
      }
    },
  );
}
