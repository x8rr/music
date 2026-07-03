import { FastifyInstance, FastifyReply } from "fastify";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { tidalSearchTracks } from "../lib/tidal";

const MUSIC_API_BASES = (
  process.env.MUSIC_API_BASES ||
  "https://hifi.geeked.wtf,https://eu-central.monochrome.tf,https://us-west.monochrome.tf,https://api.monochrome.tf,https://maus.qqdl.site,https://vogel.qqdl.site,https://katze.qqdl.site,https://hund.qqdl.site,https://monochrome-api.samidy.com,https://tidal.kinoplus.online,https://wolf.qqdl.site"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const MUSIC_STREAM_BASES = (
  process.env.MUSIC_STREAM_BASES ||
  "https://hifi.geeked.wtf,https://maus.qqdl.site,https://vogel.qqdl.site,https://katze.qqdl.site,https://hund.qqdl.site,https://wolf.qqdl.site"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const AUDIO_PROXY_BASE =
  process.env.AUDIO_PROXY_BASE || "https://audio-proxy.binimum.org/proxy-audio";

const MUSIC_TIMEOUT_MS = 4000;

const QOBUZ_BASES = (
  process.env.QOBUZ_API_BASES ||
  "https://qdl-api.monochrome.tf,https://qobuz.kennyy.com.br,https://mono.scavengerfurs.net"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// Deezer ISRC fallback (ported from Monochrome). A lossless (FLAC) source that
// slots between Qobuz and the lossy SoundCloud/YouTube fallbacks. The /stream/
// endpoint returns the audio directly, keyed by ISRC.
const DEEZER_BASES = (
  process.env.DEEZER_API_BASES || "https://dzr.tabs-vs-spaces.wtf"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// Public Deezer instances gate on an allowed Origin, so every request to one
// (resolve + the proxied stream fetch) must present an allowlisted origin.
const DEEZER_ORIGIN = process.env.DEEZER_ORIGIN || "https://monochrome.tf";

// Warm tracks probe in ~200ms and misses 404 in ~250ms, so a short timeout is
// enough to classify. A stall means a cold track (warm-up can take 90s+) — we
// hand back the URL and let the stream proxy wait it out instead of failing.
const DEEZER_TIMEOUT_MS = Number(process.env.DEEZER_TIMEOUT_MS) || 4000;

function isDeezerUrl(url: string): boolean {
  return DEEZER_BASES.some((b) => url.startsWith(b.replace(/\/+$/, "")));
}

/** Add the Deezer origin headers when fetching a Deezer URL; otherwise pass through. */
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
  source?: "tidal" | "soundcloud" | "qobuz" | "youtube";
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

type SourcePriority = "tidal" | "qobuz" | "deezer" | "soundcloud" | "youtube";

let currentSourcePriority: SourcePriority = "tidal";
const sourceLatencies: Map<SourcePriority, number> = new Map();
let lastPingTime = 0;
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function toClientAlbum(a: any): ClientAlbum {
  const artist = a.artist || (Array.isArray(a.artists) ? a.artists[0] : null) || {};
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function toClientArtist(a: any): ClientArtist {
  return {
    id: a.id,
    name: a.name || "Unknown Artist",
    picture: tidalArtistPictureUrl(a.picture),
  };
}

// Extract a named section (tracks/albums/artists) from a Tidal search response.
// The proxy wraps responses as { version, data: { tracks: { items }, albums: { items }, ... } }
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function extractSearchSection(data: any, key: string): any[] {
  if (!data || typeof data !== "object") return [];

  // Unwrap { version, data: {...} }
  if (data.data && typeof data.data === "object") {
    return extractSearchSection(data.data, key);
  }

  // Section with items array: data[key].items
  if (data[key] && Array.isArray((data[key] as any).items)) {
    return (data[key] as any).items;
  }

  // Flat items array at root (e.g. ?s= scoped track search)
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

const SC_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
// client_ids are mixed-case alphanumeric (~32 chars), not lowercase hex — the
// old /[a-f0-9]+/ regex silently failed to match real ids and 500'd.
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

    // Every JS bundle the homepage references; the client_id lives in one of
    // the later bundles, so scan from the end.
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
  const primaryArtist = t.artists && t.artists.length > 0 ? t.artists[0] : t.artist;
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
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return (await r.json()) as T;
    } catch (e) {
      clearTimeout(t);
      throw e;
    }
  };

  // Race all bases in parallel — return first success, ignore others
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
          errors.push(`${base}: ${err instanceof Error ? err.message : String(err)}`);
          remaining--;
          if (remaining === 0 && !settled) {
            reject(new Error(`All music upstreams failed: ${errors.join(" | ")}`));
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

      if (!res.ok) continue;

      const data = (await res.json()) as QobuzSearchResp;

      const items = data?.data?.tracks?.items || [];
      return items.map(
        (t): ClientTrack => ({
          id: t.id,
          title: t.title,
          artist: t.artist?.name || t.artists?.[0]?.name || "Unknown",
          album: t.album?.title,
          artwork: t.album?.cover || "",
          duration: t.duration,
          explicit: t.explicit || false,
          isrc: t.isrc,
          source: "qobuz",
        }),
      );
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

async function testSourceLatency(source: SourcePriority): Promise<number> {
  const start = Date.now();
  try {
    switch (source) {
      case "tidal": {
        await tidalSearchTracks("test", 1);
        break;
      }
      case "qobuz": {
        let success = false;
        for (const base of QOBUZ_BASES) {
          try {
            const res = await fetch(
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
        const res = await fetch(url.toString(), {
          headers: {
            "User-Agent": "Mozilla/5.0",
            Accept: "application/json",
          },
        });
        if (!res.ok) throw new Error(`SoundCloud HTTP ${res.status}`);
        break;
      }
      case "youtube": {
        const { default: yts } = await import("yt-search");
        await (yts as (q: string) => Promise<unknown>)("test audio");
        break;
      }
    }
    const latency = Date.now() - start;
    sourceLatencies.set(source, latency);
    return latency;
  } catch (err) {
    console.log(`${source} ping failed:`, err);
    sourceLatencies.set(source, Infinity);
    return Infinity;
  }
}

let pingInFlight = false;

function schedulePing() {
  const now = Date.now();
  if (pingInFlight) return;
  if (now - lastPingTime < PING_INTERVAL && sourceLatencies.size > 0) return;
  pingInFlight = true;
  pingAllSources().finally(() => { pingInFlight = false; });
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

  const results = await Promise.all([
    testSourceLatency("tidal"),
    testSourceLatency("qobuz"),
    testSourceLatency("soundcloud"),
    testSourceLatency("youtube"),
  ]);

  const workingSources: SourcePriority[] = [];
  const sourceLatencyMap: Record<string, number> = {};

  results.forEach((lat, idx) => {
    const source = ["tidal", "qobuz", "soundcloud", "youtube"][
      idx
    ] as SourcePriority;
    sourceLatencyMap[source] = lat;
    if (lat !== Infinity) {
      workingSources.push(source);
    }
  });

  if (workingSources.includes("tidal")) {
    currentSourcePriority = "tidal";
  } else if (workingSources.includes("qobuz")) {
    currentSourcePriority = "qobuz";
  } else if (workingSources.includes("soundcloud")) {
    currentSourcePriority = "soundcloud";
  } else if (workingSources.includes("youtube")) {
    currentSourcePriority = "youtube";
  }

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

  const fallbackOrder: SourcePriority[] = [
    "tidal",
    "qobuz",
    "soundcloud",
    "youtube",
  ];
  const startIndex = fallbackOrder.indexOf(sourceToUse);
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
      } else if (source === "youtube") {
        const { default: yts } = await import("yt-search");
        const ytResults = await (yts as any)(query);
        const videos = (ytResults?.videos || []).slice(0, limit);
        results = videos.map((v: any) => ({
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
}

/** Resolve a playable stream from a single source (throws if that source can't serve it). */
async function resolveFromSource(
  source: SourcePriority,
  id: string,
  isrc: string | undefined,
  quality: string | undefined,
  sourceHint: SourcePriority | undefined,
  meta: StreamMeta | undefined,
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
  if (source === "qobuz") {
    if (!isrc) throw new Error("No ISRC for Qobuz lookup");
    const r = await resolveQobuzStreamUrl(isrc);
    return { url: r.url, mimeType: "audio/flac", source: "qobuz" };
  }
  if (source === "deezer") {
    if (!isrc) throw new Error("No ISRC for Deezer lookup");
    const r = await resolveDeezerStreamUrl(isrc, quality);
    return { url: r.url, mimeType: r.mimeType, source: "deezer" };
  }
  if (source === "soundcloud") {
    // If the id is a Tidal/Qobuz id (or we have better meta), search SC by name.
    const numId = parseInt(id);
    let scTrackId: number;
    if (sourceHint !== "soundcloud" && meta?.artist && meta?.title) {
      const scResults = await soundcloudApiRequest<SoundcloudSearchResp>("/tracks", {
        q: `${meta.artist} ${meta.title}`,
        limit: 5,
      });
      const match =
        scResults.collection?.find((t) =>
          t.title.toLowerCase().includes(meta.title!.toLowerCase().split("(")[0].trim()),
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
  // youtube — search by title+artist if this isn't a native YT id.
  if (meta?.artist && meta?.title) {
    const { default: yts } = await import("yt-search");
    const ytResults = await (yts as any)(`${meta.artist} ${meta.title} audio`);
    const video = ytResults?.videos?.[0];
    if (!video?.videoId) throw new Error("No YouTube results");
    const r = await resolveYoutubeStreamUrl(video.videoId);
    return { url: r.url, mimeType: r.mimeType, source: "youtube" };
  }
  const r = await resolveYoutubeStreamUrl(id);
  return { url: r.url, mimeType: r.mimeType, source: "youtube" };
}

/**
 * Resolve a stream, preferring quality but optimizing for reliability: try the
 * hinted source first, then race the remaining lossless sources in parallel,
 * then the lossy ones. Racing means one dead/slow source can't serialize-block
 * the others — the first source that actually works wins.
 */
async function resolveStreamWithFallback(
  id: string,
  isrc?: string,
  quality?: string,
  sourceHint?: SourcePriority,
  meta?: StreamMeta,
): Promise<StreamResult> {
  const tried = new Set<SourcePriority>();
  const attempt = (s: SourcePriority) =>
    resolveFromSource(s, id, isrc, quality, sourceHint, meta).catch((e) => {
      console.error(
        `${s} stream failed:`,
        e instanceof Error ? e.message : e,
      );
      throw e;
    });

  // Resolve with the first source in the tier that succeeds, or null if all fail.
  const raceTier = async (
    sources: SourcePriority[],
  ): Promise<StreamResult | null> => {
    const pending = sources.filter((s) => !tried.has(s));
    pending.forEach((s) => tried.add(s));
    if (pending.length === 0) return null;
    try {
      return await Promise.any(pending.map(attempt));
    } catch {
      return null; // AggregateError — every source in this tier failed
    }
  };

  // 0) Deezer first when we have an ISRC — it's an exact ISRC match (most
  //    accurate; other sources can return wrong versions/remasters).
  if (isrc && !tried.has("deezer")) {
    tried.add("deezer");
    try {
      return await attempt("deezer");
    } catch {
      /* fall through */
    }
  }

  // 1) Preferred source next (where the id natively lives).
  if (sourceHint && !tried.has(sourceHint)) {
    tried.add(sourceHint);
    try {
      return await attempt(sourceHint);
    } catch {
      /* fall through to the parallel tiers */
    }
  }

  // 2) Remaining lossless sources, then 3) lossy sources — each raced in parallel.
  const lossless = await raceTier(["tidal", "qobuz", "deezer"]);
  if (lossless) return lossless;
  const lossy = await raceTier(["soundcloud", "youtube"]);
  if (lossy) return lossy;

  throw new Error("All stream sources failed");
}

// Quality token → Deezer format (mirrors Monochrome's getDeezerStreamFormat).
const DEEZER_QUALITY_FORMATS: Record<string, string> = {
  HI_RES_LOSSLESS: "FLAC",
  LOSSLESS: "FLAC",
  HIGH: "MP3_320",
  LOW: "MP3_128",
  NORMAL: "MP3_128",
};

/**
 * Resolve a stream from a Deezer ISRC-fallback instance. The /stream/ endpoint
 * serves the audio directly; we validate with a 1-byte range so a miss falls
 * through to the next source instead of handing back a URL that 404s mid-play.
 */
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
        // Timeout / network blip. A *missing* track 404s fast (~250ms), so a
        // stall means the instance is warming this track (cold start can take
        // 90s+). Hand back the URL and let the stream proxy wait it out rather
        // than failing the whole resolve.
        clearTimeout(t);
        return { url, mimeType };
      }
      clearTimeout(t);
      res.body?.cancel().catch(() => {});
      if (res.ok || res.status === 206) return { url, mimeType };
      if (res.status === 404) {
        lastErr = new Error("Deezer 404 (no match)");
        continue; // genuine miss — try next base / fall through to other sources
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

  // SoundCloud API v2 uses media.transcodings; prefer progressive (direct MP3)
  const transcodings = track.media?.transcodings || [];
  const progressive = transcodings.find((t) => t.format?.protocol === "progressive");
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        Accept: "application/json",
        Origin: "https://soundcloud.com",
        Referer: "https://soundcloud.com/",
      },
    });
    clearTimeout(timeout);
    if (!res.ok) throw new Error(`SoundCloud transcoding resolve failed: ${res.status}`);
    const data = (await res.json()) as { url: string };
    if (!data.url) throw new Error("No URL in SoundCloud transcoding response");
    const mimeType = progressive ? "audio/mpeg" : "application/vnd.apple.mpegurl";
    return { url: data.url, mimeType };
  }

  // Legacy: some tracks still have stream_url
  if (track.stream_url) {
    const streamUrl = new URL(track.stream_url);
    streamUrl.searchParams.set("client_id", clientId);
    return { url: streamUrl.toString(), mimeType: "audio/mpeg" };
  }

  throw new Error("No stream available for this SoundCloud track");
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
  // NOTE: streaming intentionally goes through the external HiFi instances
  // (which hold real Tidal user tokens). The in-process adapter only has a
  // client-credentials token, which Tidal restricts to 30-second previews —
  // useless for playback. Full-track in-process streaming needs a subscription
  // user token (OAuth device login); until then, instances + the
  // qobuz/deezer/etc. fallback chain serve full tracks.
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

async function resolveYoutubeStreamUrl(
  videoId: string,
): Promise<{ url: string; mimeType: string }> {
  const { default: youtubeDl } = await import("youtube-dl-exec");
  const videoUrl = `https://www.youtube.com/watch?v=${videoId}`;
  const info = (await youtubeDl(videoUrl, {
    format: "bestaudio",
    getUrl: true,
  })) as string;
  return {
    url: info.trim(),
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
    if (
      source &&
      ["tidal", "qobuz", "soundcloud", "youtube"].includes(source)
    ) {
      currentSourcePriority = source;
      return reply.send({
        current: currentSourcePriority,
        message: `Switched to ${source}`,
      });
    }
    return musicError(
      reply,
      400,
      "Invalid source. Use: tidal, qobuz, soundcloud, or youtube",
    );
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
        // Try ?q= first (combined search, returns all categories in one shot)
        // Fall back to parallel ?s= (tracks) + ?al= (albums) + ?a= (artists)
        let trackItems: TidalTrack[] = [];
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        let albumItems: any[] = [];
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        let artistItems: any[] = [];

        const combined = await callMusicApi<unknown>("/search/", { q: q.trim(), limit: lim }).catch(() => null);
        if (combined) {
          trackItems = extractSearchSection(combined, "tracks") as TidalTrack[];
          albumItems = extractSearchSection(combined, "albums");
          artistItems = extractSearchSection(combined, "artists");
        }

        // If combined didn't give tracks, fall back to parallel scoped searches
        if (trackItems.length === 0) {
          const [tracksResp, albumsResp, artistsResp] = await Promise.allSettled([
            callMusicApi<unknown>("/search/", { s: q.trim(), limit: lim }),
            callMusicApi<unknown>("/search/", { al: q.trim(), limit: Math.min(lim, 20) }),
            callMusicApi<unknown>("/search/", { a: q.trim(), limit: Math.min(lim, 12) }),
          ]);

          if (tracksResp.status === "fulfilled") {
            // ?s= returns flat { data: { items: [...] } }
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const raw: any = (tracksResp.value as any)?.data ?? tracksResp.value;
            if (Array.isArray(raw?.items)) trackItems = raw.items;
          }
          if (albumItems.length === 0 && albumsResp.status === "fulfilled") {
            albumItems = extractSearchSection(albumsResp.value, "albums");
          }
          if (artistItems.length === 0 && artistsResp.status === "fulfilled") {
            // ?a= might return flat items or nested artists section
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const raw: any = (artistsResp.value as any)?.data ?? artistsResp.value;
            artistItems = Array.isArray(raw?.items)
              ? raw.items
              : extractSearchSection(artistsResp.value, "artists");
          }
        }

        const tracks: ClientTrack[] = trackItems.map((t) => toClientTrack(t)).slice(0, lim);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const albums: ClientAlbum[] = albumItems.map((a: any) => toClientAlbum(a)).slice(0, 12);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const artists: ClientArtist[] = artistItems.map((a: any) => toClientArtist(a)).slice(0, 12);

        if (tracks.length > 0 || albums.length > 0 || artists.length > 0) {
          reply.header("cache-control", "public, max-age=120");
          return reply.send({ items: tracks, albums, artists, source: "tidal" });
        }
      } catch {
        // fall through to priority-based search
      }
    }

    const items = await searchWithPriority(q, lim, source);
    reply.header("cache-control", "public, max-age=120");
    return reply.send({ items, albums: [], artists: [], source: currentSourcePriority });
  });

  fastify.get("/api/music/album/:id", async (req, reply) => {
    const { id } = req.params as { id: string };
    try {
      const data = await callMusicApi<unknown>("/album/", { id });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const raw: any = (data as any)?.data ?? data;

      let albumRaw: any = null;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const raw: any = (data as any)?.data ?? data;

      const artistRaw =
        raw?.artist ||
        (Array.isArray(raw) ? raw[0] : null) ||
        (raw?.id ? raw : null);
      if (!artistRaw) return musicError(reply, 404, "Artist not found");

      const artist = toClientArtist(artistRaw);

      // Collect albums and tracks from nested response
      const albumMap = new Map<number, ClientAlbum>();
      const trackMap = new Map<number, ClientTrack>();

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const scan = (value: any, visited: Set<unknown>) => {
        if (!value || typeof value !== "object" || visited.has(value)) return;
        visited.add(value);
        if (Array.isArray(value)) {
          value.forEach((item) => scan(item, visited));
          return;
        }
        const item = value.item || value;
        const hasAlbumFields = "numberOfTracks" in item || "numberOfItems" in item;
        const hasTrackFields = item.duration && item.trackNumber != null;
        if (hasAlbumFields && item.id) albumMap.set(item.id, toClientAlbum(item));
        else if (hasTrackFields && item.id) trackMap.set(item.id, toClientTrack(item));
        Object.values(value).forEach((nested) => scan(nested, visited));
      };

      scan(raw, new Set());

      // Fallback: try /artist/?f= for albums
      if (albumMap.size === 0) {
        try {
          const albumsData = await callMusicApi<unknown>("/artist/", { f: id, skip_tracks: "true" });
          scan((albumsData as any)?.data ?? albumsData, new Set());
        } catch { /* ignore */ }
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
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const data = await callMusicApi<any>("/artist/similar/", { id });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const artists: ClientArtist[] = (data?.artists || data?.data?.artists || []).map((a: any) => toClientArtist(a));
      reply.header("cache-control", "public, max-age=600");
      return reply.send({ artists });
    } catch (error) {
      return reply.send({ artists: [] });
    }
  });

  fastify.get("/api/music/album/:id/similar", async (req, reply) => {
    const { id } = req.params as { id: string };
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const data = await callMusicApi<any>("/album/similar/", { id });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const raw = data?.data ?? data;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const items = Array.isArray(raw?.items) ? raw.items : Array.isArray(raw) ? raw : [];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const albums: ClientAlbum[] = items.map((a: any) => toClientAlbum(a));
      reply.header("cache-control", "public, max-age=600");
      return reply.send({ albums });
    } catch (error) {
      return reply.send({ albums: [] });
    }
  });

  fastify.get("/api/music/stream", async (req, reply) => {
    const { id, quality, isrc, source, title, artist } = req.query as {
      id?: string;
      quality?: string;
      isrc?: string;
      source?: SourcePriority;
      title?: string;
      artist?: string;
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
        { title, artist },
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

    let upstream: Response;
    try {
      upstream = await fetch(streamUrl, {
        headers: streamFetchHeaders(streamUrl, headers),
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
    return reply.send(
      Readable.fromWeb(upstream.body as unknown as NodeReadableStream),
    );
  });

  fastify.get("/api/music/track", async (req, reply) => {
    const { id, quality, isrc, source, title, artist } = req.query as {
      id?: string;
      quality?: string;
      isrc?: string;
      source?: SourcePriority;
      title?: string;
      artist?: string;
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
        { title, artist },
      );
      return reply.send(result);
    } catch (error) {
      return musicError(reply, 502, "All stream sources failed", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  });

  fastify.get("/api/music/manifest", async (req, reply) => {
    const { id, quality, isrc, artist, title, source } = req.query as {
      id?: string;
      quality?: string;
      isrc?: string;
      artist?: string;
      title?: string;
      source?: SourcePriority;
    };

    if (!id) return musicError(reply, 400, "Missing required parameter: id");

    schedulePing();
    const sourceToUse = source || currentSourcePriority;

    // Deezer first — exact ISRC match (most accurate). Non-DASH: the client
    // plays it through /api/music/stream. Falls through on miss.
    if (isrc) {
      try {
        const d = await resolveDeezerStreamUrl(isrc, quality);
        reply.header("cache-control", "no-store");
        return reply.send({ url: d.url, mimeType: d.mimeType, source: "deezer" });
      } catch {
        /* fall through to other sources */
      }
    }

    try {
      const result = await resolveStreamWithFallback(
        id,
        isrc,
        quality,
        sourceToUse,
        { title, artist },
      );

      if (result.source === "tidal" && (result.isDash || result.rawDash)) {
        let rawDash = result.rawDash;
        if (!rawDash) {
          const mpdRes = await fetch(result.url);
          rawDash = await mpdRes.text();
        }
        const rewritten = rawDash.replace(
          /(initialization|media)="(https:\/\/[^"]+)"/g,
          (_, attr, url) => `${attr}="${AUDIO_PROXY_BASE}/${url}"`,
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
