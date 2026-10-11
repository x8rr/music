import { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";
import { createReadStream, promises as fsp } from "node:fs";
import { tidalApi, tidalPlaylistTracks, tidalSearchTracks } from "../lib/tidal";
import { proxyUpstreamRanged } from "../lib/rangeProxy";
import musicSources from "../config/music-sources.json";
import {
  isYtmVideoId,
  ytmAudioFile,
  ytmBase,
  ytmConfigured,
  ytmMimeType,
  ytmResolveVideoId,
  ytmSearch,
  type YtmTrack,
} from "../lib/ytm";
import { getArt, isAllowedArtUrl, openArt } from "../lib/artCache";
import { touchIfStale } from "../lib/cacheSweep";
import { r2 } from "../lib/r2";
import { musicSourceBreaker, SourceParkedError } from "../lib/sourceBreaker";
import { LruCache } from "../lib/lruCache";

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
const MUSIC_TIMEOUT_MS = 4000;

// The ytm source resolves to an audio file this same server holds on disk, and
// every consumer of a StreamResult (the playability probe, the /api/music
// stream proxy) takes an absolute URL. Point them back at our own listener
// rather than at the public hostname: a loopback hop costs nothing and keeps
// working when the deploy sits behind Caddy under a name we don't know here.
const SELF_ORIGIN = `http://127.0.0.1:${Number(process.env.MUSIC_PORT) || 2010}`;

// Tidal only supplies search results and metadata: its own playback resolves
// to 30-second previews, so every stream is resolved by ytm.

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
  releaseDate?: string;
  explicit?: boolean;
  audioModes?: string[];
  artist?: TidalArtist;
  artists?: TidalArtist[];
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

type SourcePriority = "tidal" | "ytm";
/** Sources that search but never stream. Tidal is the whole catalogue as far
 * as the UI is concerned — track ids, album and artist pages, artwork and
 * ISRCs all come from it — but its own playback resolves to 30-second
 * previews, so it is kept out of the stream race and the audio for a Tidal
 * search result comes from whichever real source wins below. */
const SEARCH_ONLY_SOURCES: ReadonlySet<SourcePriority> = new Set<SourcePriority>(
  ["tidal"],
);

/** Canonical source order. Streaming races all of these at once and plays
 * whichever resolves first, so this order only decides search fallback and
 * ties in the UI — but it is the one place the intended ranking is written
 * down, so keep it in sync with the picker in Music.tsx. */
const SOURCE_ORDER: SourcePriority[] = ["ytm", "tidal"];

/** Where audio comes from, in tiers: each tier is raced, and the next one is
 * only tried when the whole tier fails. YouTube Music is the only one: a
 * source that matches by name has to be strict about the artist, and a wrong
 * track is worse than a failed one (Qobuz matched "Cellar" by title and
 * length alone and played a different artist's song). Tidal is the catalogue
 * (names, albums, covers) and never plays. */
const STREAM_TIERS: SourcePriority[][] = [["ytm"]];

/** YouTube Music has no built-in host: until YTM_BASE is set it is neither
 * searched, pinged nor raced, and Tidal answers search on its own. */
function sourceConfigured(s: SourcePriority): boolean {
  return s !== "ytm" || ytmConfigured();
}

let currentSourcePriority: SourcePriority = ytmConfigured() ? "ytm" : "tidal";
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
  return { ...streamProxy, sources: musicSourceBreaker.stats() };
}
const PING_INTERVAL = 300000;

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

function musicError(
  reply: FastifyReply,
  status: number,
  message: string,
  extra?: Record<string, unknown>,
) {
  reply.code(status);
  return reply.send({ error: message, ...extra });
}

function toYtmClientTrack(t: YtmTrack): ClientTrack {
  return {
    id: t.id,
    title: t.title,
    artist: t.artist || "Unknown",
    album: t.album || undefined,
    artwork: t.thumb,
    duration: t.duration ?? 0,
    explicit: false,
    source: "ytm",
    permalinkUrl: `${ytmBase() ?? ""}/watch?v=${t.id}`,
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
  };
}

async function callMusicApi<T>(
  path: string,
  params: Record<string, string | number | undefined> = {},
  bases: string[] = MUSIC_API_BASES,
): Promise<T> {
  // Every base is asked at once and the first answer wins; the rest are
  // aborted rather than read to completion, which used to pull every
  // upstream's full body into memory for nothing.
  const controllers: AbortController[] = [];
  const makeRequest = async (base: string): Promise<T> => {
    const url = new URL(path, base);
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined) url.searchParams.set(k, String(v));
    }
    const ctrl = new AbortController();
    controllers.push(ctrl);
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
            for (const c of controllers) c.abort();
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

// Health probes run under Promise.all, so one hanging upstream stalls the whole
// ping — and /api/music/source and /api/music/ping along with it. A dead mirror
// that accepts the connection and never answers would otherwise hold the
// request open indefinitely.
const PING_TOTAL_TIMEOUT_MS = 8000;

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
      case "ytm": {
        // Innertube only, not yt-dlp: a healthy search is what decides whether
        // the source can find anything, and shelling out to download a track
        // every five minutes just to prove the binary exists is not a ping.
        await ytmSearch("test", 1);
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

  const pinged = SOURCE_ORDER.filter(sourceConfigured);
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

// Album and artist pages go to api.tidal.com first. They used to go only
// through the hifi mirrors, and by 2026-09-29 one of eleven still answered.
// The mirror stays as the fallback.
const TIDAL_ID_RE = /^\d{1,12}$/;
export const LOOKUP_MAX_LENGTH = 200;

// Every search, album page and name lookup asks api.tidal.com from this one
// box, and listeners open the same few albums and repeat the same queries, so
// an answer is kept for a while instead of asked for again per request. A
// failure is not kept.
const tidalCache = new LruCache<unknown>("tidalCatalog", 3000, 20 * 60_000);
const tidalInFlight = new Map<string, Promise<unknown>>();
function cachedTidal<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const hit = tidalCache.get(key);
  if (hit !== undefined) return Promise.resolve(hit as T);
  // Concurrent asks for the same thing share one request.
  let job = tidalInFlight.get(key) as Promise<T> | undefined;
  if (!job) {
    job = fn()
      .then((value) => {
        tidalCache.set(key, value);
        return value;
      })
      .finally(() => tidalInFlight.delete(key));
    tidalInFlight.set(key, job);
  }
  return job;
}

const isNotFound = (err: unknown) => err instanceof Error && /→ 404$/.test(err.message);

type TidalPage<T> = { items?: T[]; totalNumberOfItems?: number };

function tidalAlbumPage(id: string): Promise<{ album: ClientAlbum; tracks: ClientTrack[] }> {
  return cachedTidal(`album:${id}`, () => fetchTidalAlbumPage(id));
}

async function fetchTidalAlbumPage(id: string): Promise<{ album: ClientAlbum; tracks: ClientTrack[] }> {
  type Item = { item?: TidalTrack; type?: string };
  const [albumRaw, first] = await Promise.all([
    tidalApi<TidalAlbum>(`/v1/albums/${id}`),
    tidalApi<TidalPage<Item>>(`/v1/albums/${id}/items`, { limit: 100 }),
  ]);
  const items = [...(first.items ?? [])];
  // Box sets run past one page; 500 tracks is plenty for a page anyone scrolls.
  for (let offset = 100; offset < Math.min(first.totalNumberOfItems ?? 0, 500); offset += 100)
    items.push(...((await tidalApi<TidalPage<Item>>(`/v1/albums/${id}/items`, { limit: 100, offset })).items ?? []));
  return {
    album: toClientAlbum(albumRaw),
    // Albums can carry music videos between the tracks.
    tracks: items.filter((i) => i.type === "track" && i.item?.id).map((i) => toClientTrack(i.item!)),
  };
}

const byReleaseDesc = (a: ClientAlbum, b: ClientAlbum) => (b.releaseDate || "").localeCompare(a.releaseDate || "");

/** Tidal lists every edition of a release (explicit, clean, Dolby Atmos,
 * regional) as its own album, so an artist page showed "Man's Best Friend
 * (Bonus Track Version)" six times. Editions share a release date, which
 * keeps two different "Greatest Hits" apart. Each release keeps the place of
 * its first edition and shows its explicit stereo one, the original. */
export function uniqueReleases(albums: TidalAlbum[]): ClientAlbum[] {
  const best = new Map<string, TidalAlbum>();
  const rank = (a: TidalAlbum) => (a.explicit ? 2 : 0) + (a.audioModes?.includes("STEREO") ? 1 : 0);
  for (const a of albums) {
    const c = toClientAlbum(a);
    const key = `${c.artist.name}|${c.title}|${c.releaseDate ?? ""}`.toLowerCase().replace(/[‘’]/g, "'");
    const kept = best.get(key);
    if (!kept) best.set(key, a);
    else if (rank(a) > rank(kept)) {
      // Re-setting an existing key keeps its insertion position.
      best.set(key, a);
    }
  }
  return [...best.values()].map(toClientAlbum);
}

function tidalArtistPage(id: string) {
  return cachedTidal(`artist:${id}`, () => fetchTidalArtistPage(id));
}

async function fetchTidalArtistPage(id: string) {
  const [artistRaw, albums, eps, top] = await Promise.all([
    tidalApi<TidalArtist>(`/v1/artists/${id}`),
    tidalApi<TidalPage<TidalAlbum>>(`/v1/artists/${id}/albums`, { limit: 50 }),
    tidalApi<TidalPage<TidalAlbum>>(`/v1/artists/${id}/albums`, { filter: "EPSANDSINGLES", limit: 50 }),
    tidalApi<TidalPage<TidalTrack>>(`/v1/artists/${id}/toptracks`, { limit: 15 }),
  ]);
  return {
    artist: toClientArtist(artistRaw),
    albums: uniqueReleases(albums.items ?? []).sort(byReleaseDesc),
    eps: uniqueReleases(eps.items ?? []).sort(byReleaseDesc),
    tracks: (top.items ?? []).map(toClientTrack),
  };
}

/** Tidal has no similar-albums answer for most singles and EPs; the artist's
 * other releases are the next best thing to show under one. */
function tidalSimilarAlbums(id: string): Promise<ClientAlbum[]> {
  return cachedTidal(`similar-albums:${id}`, () => fetchTidalSimilarAlbums(id));
}

async function fetchTidalSimilarAlbums(id: string): Promise<ClientAlbum[]> {
  // Only "none" (Tidal answers 404) falls back; a timeout or 429 must fail
  // rather than get cached as this album's answer.
  const similar = await tidalApi<TidalPage<TidalAlbum>>(`/v1/albums/${id}/similar`, { limit: 12 }).catch((err) => {
    if (isNotFound(err)) return null;
    throw err;
  });
  if (similar?.items?.length) return uniqueReleases(similar.items);
  const album = toClientAlbum(await tidalApi<TidalAlbum>(`/v1/albums/${id}`));
  if (!album.artist.id) return [];
  const [albums, eps] = await Promise.all([
    tidalApi<TidalPage<TidalAlbum>>(`/v1/artists/${album.artist.id}/albums`, { limit: 12 }),
    tidalApi<TidalPage<TidalAlbum>>(`/v1/artists/${album.artist.id}/albums`, { filter: "EPSANDSINGLES", limit: 12 }),
  ]);
  return uniqueReleases([...(albums.items ?? []), ...(eps.items ?? [])])
    .filter((a) => String(a.id) !== id && a.title.toLowerCase() !== album.title.toLowerCase())
    .slice(0, 12);
}

// Non-Latin titles normalise to nothing, and "" would match every candidate.
const matchKey = (s: string) => normalizeForMatch(s) || s.toLowerCase().trim();
/** "Drake, 21 Savage" and "Billie Eilish & Justin Bieber" lead with the artist
 * Tidal files the release under. So does "Tyler, The Creator", which is why
 * the full name is always tried first and the split one only after. */
const primaryArtist = (s: string) => s.split(/\s*(?:,|&|\bx\b|\bfeat\.?|\bft\.?|\bwith\b)\s*/i)[0]?.trim() ?? "";
/** The names to try for an artist string, most specific first. "Unknown" is
 * what a source without an artist fills in, and is no name at all. */
function artistNames(s: string): string[] {
  const full = s.trim();
  if (!full || /^unknown( artist)?$/i.test(full)) return [];
  const primary = primaryArtist(full);
  return primary && primary !== full ? [full, primary] : [full];
}

/** The Tidal album a track from another source (YouTube Music,
 * SoundCloud) belongs to, by name: exact title and artist first, then exact
 * title, then an edition of it ("... (Deluxe)"). */
export function lookupTidalAlbum(artist: string, title: string): Promise<number | null> {
  return cachedTidal(`lookup-album:${artist.toLowerCase()}|${title.toLowerCase()}`, () => findTidalAlbum(artist, title));
}

async function findTidalAlbum(artist: string, title: string): Promise<number | null> {
  type Hit = TidalAlbum & { artist?: TidalArtist; artists?: TidalArtist[] };
  const names = artistNames(artist);
  const wanted = new Set(names.map(matchKey));
  const wantTitle = matchKey(title);
  const byArtist = (a: Hit) =>
    [a.artist, ...(a.artists ?? [])].some((x) => x?.name && wanted.has(matchKey(x.name)));
  // Without an artist only an exact title is trusted; with one, never another
  // artist's album, but an edition of the right one ("... (Deluxe)") is fine.
  for (const name of names.length ? names : [""]) {
    const { items = [] } = await tidalApi<TidalPage<Hit>>("/v1/search/albums", {
      query: `${name} ${title}`.trim(),
      limit: 10,
    });
    const exact = items.filter((a) => matchKey(a.title) === wantTitle);
    const pick = names.length
      ? (exact.find(byArtist) ?? items.find((a) => byArtist(a) && matchKey(a.title).startsWith(wantTitle)))
      : exact[0];
    if (pick) return pick.id;
  }
  return null;
}

export function lookupTidalArtist(name: string): Promise<number | null> {
  return cachedTidal(`lookup-artist:${name.toLowerCase()}`, () => findTidalArtist(name));
}

async function findTidalArtist(name: string): Promise<number | null> {
  for (const candidate of artistNames(name)) {
    const want = matchKey(candidate);
    const { items = [] } = await tidalApi<TidalPage<TidalArtist>>("/v1/search/artists", { query: candidate, limit: 5 });
    const hit = items.find((a) => matchKey(a.name) === want);
    if (hit) return hit.id;
  }
  return null;
}

/** Albums and artists for a search come from Tidal whatever the track source
 * is, since the album and artist pages are Tidal's. They used to show only
 * when the listener had picked Tidal, so most people never saw an album. */
// The track search never waits on this longer than the budget; after a
// failure Tidal is left alone briefly instead of costing every search again.
const CATALOG_BUDGET_MS = 2000;
const CATALOG_PAUSE_MS = 30_000;
let catalogPausedUntil = 0;
async function searchCatalog(q: string): Promise<{ albums: ClientAlbum[]; artists: ClientArtist[] }> {
  const none = { albums: [], artists: [] };
  if (q.length > LOOKUP_MAX_LENGTH || Date.now() < catalogPausedUntil) return none;
  const job = cachedTidal(`search:${q.toLowerCase()}`, async () => {
    const r = await tidalApi<{ albums?: TidalPage<TidalAlbum>; artists?: TidalPage<TidalArtist> }>("/v1/search", {
      query: q,
      types: "ALBUMS,ARTISTS",
      limit: 12,
    });
    return { albums: uniqueReleases(r.albums?.items ?? []), artists: (r.artists?.items ?? []).map(toClientArtist) };
  }).catch(() => {
    catalogPausedUntil = Date.now() + CATALOG_PAUSE_MS;
    return none;
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const late = new Promise<typeof none>((resolve) => (timer = setTimeout(() => resolve(none), CATALOG_BUDGET_MS)));
  // A late answer still lands in the cache for the next search.
  return Promise.race([job, late]).finally(() => clearTimeout(timer));
}

/** Tidal editorial playlists behind the Music page's browse rows, keyed like
 * BROWSE_ROWS in Music.tsx. The rows used to be keyword searches ("top hits
 * 2025"), which on YouTube Music meant random songs with those words in the
 * title, and cost ten searches per visit. The uuids have been stable since
 * 2018-20 and the playlists are refreshed weekly. */
const BROWSE_PLAYLISTS: Record<string, string> = {
  hits: "edf3b7d2-cb42-41d7-93c0-afa2a395521b",
  pop: "36ea71a8-445e-41a4-82ab-6628c581535d",
  hiphop: "406948b1-a8d4-493d-9672-90df47580540",
  indie: "13f9d6c8-58e6-4869-8d9b-000d0ff95f0b",
  rock: "94fe2b9b-096d-4b39-8129-d5b8e774e9b3",
  rnb: "3dd419f2-ab69-4c94-b418-9e8f7595c684",
  latin: "e4d5a592-5957-421c-8106-a28616a5feff",
  electronic: "4261748a-4287-4758-aaab-6d5be3e99e52",
  kpop: "ef02ddf5-f1c3-46cb-8110-8cdc797fce82",
  chill: "f2e0f969-2e48-49bc-b03f-9354ad50fe14",
};
const BROWSE_ROW_LIMIT = 30;
const BROWSE_TTL_MS = 30 * 60_000;
type Rows = Record<string, ClientTrack[]>;
type Browse = { rows: Rows; albums: ClientAlbum[] };
let browseCache: { at: number; rows: Rows } | null = null;
let browseJob: Promise<Rows> | null = null;

/** A row whose playlist fails comes back empty and the client falls back to
 * its keyword search for it; a set with any empty row is not cached, so the
 * next visit asks Tidal again. `albums` is Tidal's new-releases feed, cached
 * on its own so a failing feed doesn't keep the rows from being cached. */
export async function browseRows(): Promise<Browse> {
  const albums = cachedTidal("new-albums", async () =>
    uniqueReleases((await tidalApi<TidalPage<TidalAlbum>>("/v1/featured/new/albums", { limit: 20 })).items ?? []),
  ).catch(() => [] as ClientAlbum[]);
  return { rows: await playlistRows(), albums: await albums };
}

function playlistRows(): Promise<Rows> {
  if (browseCache && Date.now() - browseCache.at < BROWSE_TTL_MS) return Promise.resolve(browseCache.rows);
  browseJob ??= (async () => {
    const entries = await Promise.all(
      Object.entries(BROWSE_PLAYLISTS).map(async ([key, uuid]) => {
        try {
          const { items } = await tidalPlaylistTracks(uuid, BROWSE_ROW_LIMIT);
          return [key, ((items ?? []) as TidalTrack[]).map(toClientTrack)] as const;
        } catch (err) {
          console.warn(`[music] browse row ${key} failed:`, err instanceof Error ? err.message : err);
          return [key, [] as ClientTrack[]] as const;
        }
      }),
    );
    const rows = Object.fromEntries(entries);
    if (entries.every(([, tracks]) => tracks.length > 0)) browseCache = { at: Date.now(), rows };
    return rows;
  })().finally(() => (browseJob = null));
  return browseJob;
}

async function searchWithPriority(
  query: string,
  limit: number,
  requestedSource?: SourcePriority,
): Promise<ClientTrack[]> {
  schedulePing();

  const sourceToUse = requestedSource || currentSourcePriority;

  const fallbackOrder: SourcePriority[] = SOURCE_ORDER.filter(sourceConfigured);
  const startIndex = Math.max(0, fallbackOrder.indexOf(sourceToUse));
  const orderedSources = [
    ...fallbackOrder.slice(startIndex),
    ...fallbackOrder.slice(0, startIndex),
  ];

  for (const source of orderedSources) {
    if (musicSourceBreaker.isOpen(source)) continue;
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
      } else if (source === "ytm") {
        results = (await ytmSearch(query, limit)).map(toYtmClientTrack);
      }

      musicSourceBreaker.recordSuccess(source);
      if (results.length > 0) {
        return results;
      }
    } catch (error) {
      // One line, and only for the first few of a streak: a dead source used
      // to write the same line for every search until someone fixed it.
      if (musicSourceBreaker.recordFailure(source, error))
        console.error(
          `${source} search failed: ${error instanceof Error ? error.message : String(error)}`,
        );
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
  /** A complete copy of the audio on this box, when the source keeps one.
   * /api/music/stream serves it from disk rather than proxying `url`. */
  file?: string;
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
): Promise<StreamResult> {
  if (source === "tidal") {
    // Unreachable via the race (tidal is in SEARCH_ONLY_SOURCES), but a stale
    // client can still pass source=tidal explicitly. Say so rather than
    // handing back a 30-second preview.
    throw new Error("Tidal is a search source only — it does not stream");
  }
  if (source === "ytm") {
    // A ytm-sourced track already carries a YT Music video id; anything else
    // (a Tidal search result, usually) has to be matched by name first. Both
    // conditions matter — an 11-character id is not proof on its own, and the
    // hint alone doesn't survive a malformed request.
    const videoId =
      sourceHint === "ytm" && isYtmVideoId(id)
        ? id
        : meta?.artist && meta?.title
          ? await ytmResolveVideoId(meta.artist, meta.title, meta.duration)
          : null;
    if (!videoId) throw new Error("No YouTube Music match for this track");

    // Download to the local cache before claiming the source is ready. The
    // alternative — handing back a googlevideo URL — is what made the old
    // `youtube` source unseekable: those URLs are session-bound and start
    // 403ing partway through a track. Once this resolves, the URL below is a
    // complete file on our own disk, so the playability probe and every
    // subsequent range request are local reads.
    const file = await ytmAudioFile(videoId);
    return {
      url: `${SELF_ORIGIN}/api/music/ytm/audio/${videoId}`,
      mimeType: ytmMimeType(file),
      source: "ytm",
      file,
    };
  }
  throw new Error(`Unknown stream source: ${source}`);
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
  const timer = setTimeout(() => ctrl.abort(), PLAYABILITY_TIMEOUT_MS);
  try {
    const probe = await fetch(result.url, {
      headers: { range: "bytes=0-1" },
      signal: ctrl.signal,
    });
    if (!probe.ok && probe.status !== 206) {
      throw new Error(`upstream returned ${probe.status}`);
    }
    await probe.body?.cancel();
  } finally {
    clearTimeout(timer);
  }
}

/** Whether racing `s` can plausibly produce audio for this request, decided
 * locally so a source that cannot possibly match doesn't cost a round trip —
 * or, for ytm, a yt-dlp download. */
function streamCandidateAllowed(
  s: SourcePriority,
  sourceHint: SourcePriority | undefined,
  meta?: StreamMeta,
  _isrc?: string,
): boolean {
  if (SEARCH_ONLY_SOURCES.has(s) || !sourceConfigured(s)) return false;
  // ytm needs either its own video id or a name to search innertube with.
  if (s === "ytm" && sourceHint !== "ytm" && !(meta?.title && meta?.artist))
    return false;
  return true;
}

async function resolveStreamWithFallback(
  id: string,
  isrc?: string,
  quality?: string,
  sourceHint?: SourcePriority,
  meta?: StreamMeta,
): Promise<StreamResult> {
  const started = Date.now();
  const attempt = async (s: SourcePriority) => {
    // A parked source loses the race at once: no network, no log line.
    musicSourceBreaker.assertAvailable(s);
    try {
      const result = await resolveFromSource(
        s,
        id,
        isrc,
        quality,
        sourceHint,
        meta,
      );
      await assertPlayable(result, meta);
      musicSourceBreaker.recordSuccess(s);
      console.log(`[stream] ${s} ready in ${Date.now() - started}ms`);
      return result;
    } catch (e) {
      if (!(e instanceof SourceParkedError) && musicSourceBreaker.recordFailure(s, e))
        console.error(`${s} stream failed:`, e instanceof Error ? e.message : e);
      throw e;
    }
  };

  // Within a tier every source is raced and the first *verified playable*
  // stream wins. A source that needs an ISRC it wasn't given rejects
  // immediately and costs nothing.
  const failures: string[] = [];
  for (const tier of STREAM_TIERS) {
    const candidates = tier.filter((s) => streamCandidateAllowed(s, sourceHint, meta, isrc));
    if (candidates.length === 0) continue;
    try {
      return await Promise.any(candidates.map(attempt));
    } catch (e) {
      if (e instanceof AggregateError) {
        e.errors.forEach((err, i) =>
          failures.push(`${candidates[i]}: ${err instanceof Error ? err.message : err}`),
        );
      } else failures.push(String(e));
    }
  }
  if (!ytmConfigured())
    throw new Error("No stream source is configured: set YTM_BASE");
  if (failures.length === 0) throw new Error("No usable stream sources");
  throw new Error(`All stream sources failed — ${failures.join(" | ")}`);
}

function normalizeForMatch(s: string): string {
  return s
    .toLowerCase()
    .replace(/\([^)]*\)|\[[^\]]*\]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/** Serves a complete local audio file with Range support. A Node file stream
 * through Fastify honours write backpressure, so a paused player costs nothing
 * here — unlike the fetch-based proxy below. */
async function sendLocalAudio(
  req: FastifyRequest,
  reply: FastifyReply,
  file: string,
  contentType: string,
  cacheControl: string,
) {
  let size: number;
  try {
    const st = await fsp.stat(file);
    size = st.size;
    touchIfStale(file, st);
  } catch (e) {
    return musicError(reply, 502, "Audio fetch failed", {
      detail: e instanceof Error ? e.message : String(e),
    });
  }

  reply.header("accept-ranges", "bytes");
  reply.header("content-type", contentType);
  reply.header("cache-control", cacheControl);
  reply.header("access-control-allow-origin", "*");

  const range = req.headers.range;
  if (typeof range !== "string") {
    reply.header("content-length", String(size));
    return reply.send(createReadStream(file));
  }

  const match = /^bytes=(\d*)-(\d*)$/.exec(range);
  if (!match) return reply.code(416).send();
  const start = Number(match[1] || 0);
  const end = Math.min(Number(match[2] || size - 1), size - 1);
  if (!Number.isFinite(start) || start > end || start >= size) {
    reply.header("content-range", `bytes */${size}`);
    return reply.code(416).send();
  }

  reply.code(206);
  reply.header("content-range", `bytes ${start}-${end}/${size}`);
  reply.header("content-length", String(end - start + 1));
  return reply.send(createReadStream(file, { start, end }));
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
      configured: { tidal: true, ytm: ytmConfigured(), r2: r2 !== null },
    });
  });

  fastify.post("/api/music/ping", async (req, reply) => {
    schedulePing();
    return reply.send({
      current: currentSourcePriority,
      latencies: Object.fromEntries(sourceLatencies),
    });
  });

  fastify.get("/api/music/browse", async (_req, reply) => {
    const browse = await browseRows();
    // A partial answer (a row or the album feed failed) is not kept by the
    // browser for ten minutes.
    const whole = browse.albums.length > 0 && Object.values(browse.rows).every((r) => r.length > 0);
    reply.header("cache-control", whole ? "public, max-age=600" : "no-store");
    return reply.send(browse);
  });

  fastify.get("/api/music/search", async (req, reply) => {
    const { q, limit, source } = req.query as {
      q?: string;
      limit?: string;
      source?: SourcePriority;
    };
    if (!q?.trim()) return reply.send({ items: [], albums: [], artists: [] });

    const lim = parseInt(limit ?? "30");
    const [items, catalog] = await Promise.all([searchWithPriority(q, lim, source), searchCatalog(q.trim())]);
    reply.header("cache-control", "public, max-age=120");
    return reply.send({ items, ...catalog, source: currentSourcePriority });
  });

  // A track from YouTube Music has no Tidal ids, so its
  // album and artist links find the Tidal page by name.
  fastify.get("/api/music/lookup/album", async (req, reply) => {
    const q = req.query as { artist?: string; title?: string };
    const artist = String(q.artist ?? "").trim();
    const title = String(q.title ?? "").trim();
    if (!title || title.length > LOOKUP_MAX_LENGTH || artist.length > LOOKUP_MAX_LENGTH)
      return musicError(reply, 400, "Album title required");
    try {
      const id = await lookupTidalAlbum(artist, title);
      if (!id) return musicError(reply, 404, "No matching album");
      reply.header("cache-control", "public, max-age=3600");
      return reply.send({ id });
    } catch {
      return musicError(reply, 502, "Album lookup failed");
    }
  });

  fastify.get("/api/music/lookup/artist", async (req, reply) => {
    const name = String((req.query as { name?: string }).name ?? "").trim();
    if (!name || name.length > LOOKUP_MAX_LENGTH) return musicError(reply, 400, "Artist name required");
    try {
      const id = await lookupTidalArtist(name);
      if (!id) return musicError(reply, 404, "No matching artist");
      reply.header("cache-control", "public, max-age=3600");
      return reply.send({ id });
    } catch {
      return musicError(reply, 502, "Artist lookup failed");
    }
  });

  fastify.get("/api/music/album/:id", async (req, reply) => {
    const { id } = req.params as { id: string };
    if (!TIDAL_ID_RE.test(id)) return musicError(reply, 400, "Invalid album id");
    try {
      const page = await tidalAlbumPage(id);
      reply.header("cache-control", "public, max-age=300");
      return reply.send(page);
    } catch (err) {
      console.warn(`[music] tidal album ${id} failed, trying the mirrors:`, err instanceof Error ? err.message : err);
    }
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
    if (!TIDAL_ID_RE.test(id)) return musicError(reply, 400, "Invalid artist id");
    try {
      const page = await tidalArtistPage(id);
      reply.header("cache-control", "public, max-age=300");
      return reply.send(page);
    } catch (err) {
      console.warn(`[music] tidal artist ${id} failed, trying the mirrors:`, err instanceof Error ? err.message : err);
    }
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
    if (!TIDAL_ID_RE.test(id)) return musicError(reply, 400, "Invalid artist id");
    try {
      const artists = await cachedTidal(`similar-artists:${id}`, async () => {
        const { items = [] } = await tidalApi<TidalPage<TidalArtist>>(`/v1/artists/${id}/similar`, { limit: 12 });
        return items.map(toClientArtist);
      });
      reply.header("cache-control", "public, max-age=600");
      return reply.send({ artists });
    } catch {
      // mirror below
    }
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
    if (!TIDAL_ID_RE.test(id)) return musicError(reply, 400, "Invalid album id");
    try {
      const albums = await tidalSimilarAlbums(id);
      reply.header("cache-control", "public, max-age=600");
      return reply.send({ albums });
    } catch {
      // mirror below
    }
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

  // ytm resolves to a file this server downloaded with yt-dlp, and everything
  // downstream of resolveFromSource wants a URL — so serve the cache over HTTP
  // here, with real range support. /api/music/stream then proxies this the
  // same way it proxies any other upstream, and seeking is a local file read
  // instead of a re-signed googlevideo request that can expire mid-track.
  // Album art, fetched once by us and served from disk. See lib/artCache.ts —
  // the providers rate-limit a browse grid's worth of parallel cover requests
  // and answer 429 with an HTML page, which breaks both <img> tags and the
  // artwork Chrome fetches for the OS media controls.
  fastify.get(
    "/api/music/art",
    { config: { rateLimit: { max: 300, timeWindow: "1 minute" } } },
    async (req, reply) => {
    const { u } = req.query as { u?: string };
    if (!u) return musicError(reply, 400, "Missing required parameter: u");
    if (!isAllowedArtUrl(u))
      return musicError(reply, 400, "Artwork host not allowed");

    try {
      const art = await getArt(u);
      reply.header("content-type", art.contentType);
      reply.header("content-length", String(art.size));
      // Immutable in practice: these URLs carry a content hash or an id.
      reply.header("cache-control", "public, max-age=604800, immutable");
      // Harmless now that the shell no longer sets COEP, but kept so the
      // artwork keeps loading if isolation is ever restored.
      reply.header("cross-origin-resource-policy", "cross-origin");
      reply.header("access-control-allow-origin", "*");
      // Streamed from disk: at a dozen covers a second, reading each one
      // into a Buffer first was steady native churn for no reason.
      return reply.send(openArt(art));
    } catch (error) {
      return musicError(reply, 502, "Artwork fetch failed", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  },
  );

  fastify.get("/api/music/ytm/audio/:id", async (req, reply) => {
    const { id } = req.params as { id: string };
    if (!isYtmVideoId(id)) return musicError(reply, 400, "Invalid video id");

    let file: string;
    try {
      file = await ytmAudioFile(id);
    } catch (e) {
      return musicError(reply, 502, "Audio fetch failed", {
        detail: e instanceof Error ? e.message : String(e),
      });
    }

    // The cache entry is immutable once renamed into place, so this is safe to
    // hold for a day; it is also what keeps a seek-heavy session from
    // re-reading the whole file through the proxy on every scrub.
    return sendLocalAudio(req, reply, file, ytmMimeType(file), "public, max-age=86400");
  });

  // A resolved stream is good for a while (signed upstream URLs last well
  // over five minutes; local files are immutable). <audio> issues a fresh
  // request for every Range it wants — every seek, every buffer refill — and
  // each one re-ran the whole search-and-probe race, so a seek-heavy listener
  // was worth several YouTube searches a minute.
  const STREAM_CACHE_TTL_MS = 5 * 60_000;
  const resolvedStreams = new LruCache<StreamResult>("resolvedStreams", 2000, STREAM_CACHE_TTL_MS);
  const resolving = new Map<string, Promise<StreamResult>>();

  fastify.get("/api/music/stream", async (req, reply) => {
    const { id, quality, isrc, source, title, artist, duration } = req.query as {
      id?: string;
      quality?: string;
      isrc?: string;
      source?: SourcePriority;
      title?: string;
      artist?: string;
      duration?: string;
    };
    if (!id) return musicError(reply, 400, "Missing required parameter: id");

    schedulePing();
    const sourceToUse = source || currentSourcePriority;
    const cacheKey = `${sourceToUse}:${id}:${isrc ?? ""}:${quality ?? ""}`;

    let result: StreamResult;
    try {
      const hit = resolvedStreams.get(cacheKey);
      if (hit) result = hit;
      else {
        let job = resolving.get(cacheKey);
        if (!job) {
          job = resolveStreamWithFallback(
            id,
            isrc,
            quality,
            sourceToUse,
            { title, artist, duration: duration ? Number(duration) : undefined },
          );
          resolving.set(cacheKey, job);
          job.then((r) => resolvedStreams.set(cacheKey, r)).catch(() => {}).finally(() => resolving.delete(cacheKey));
        }
        result = await job;
      }
    } catch (error) {
      return musicError(reply, 502, "All stream sources failed", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }

    // ytm ends in a complete file on this disk. Read it here
    // rather than fetching it back from our own listener: the loopback hop
    // is where a whole track per listener used to sit in memory.
    if (result.file) {
      const local = await fsp.stat(result.file).then((s) => s.size > 0, () => false);
      if (local) return sendLocalAudio(req, reply, result.file, result.mimeType, "no-store");
    }

    // Remote upstream. Bun's fetch pulls a body into memory with no
    // backpressure, so a plain pipe holds the entire track for as long as the
    // player keeps the connection open. The proxy asks the upstream for one
    // chunk at a time instead, and a client that goes away (skip, seek, tab
    // closed) aborts the chunk in flight.
    const abort = new AbortController();
    req.raw.on("close", () => abort.abort());

    // Counted so /api/system/memory can show whether streams are actually
    // being torn down. A rising `open` that never falls means the proxy is
    // holding requests for clients that are no longer there.
    streamProxy.open++;
    streamProxy.started++;
    try {
      await proxyUpstreamRanged(req, reply, result.url, {
        signal: abort.signal,
        fallbackContentType: result.mimeType || "audio/mp4",
        responseHeaders: {
          "cache-control": "no-store",
          // The player requests this in CORS mode (crossOrigin="anonymous") so
          // the equalizer's Web Audio graph may read it. Without an ACAO header
          // on a cross-origin build the browser mutes it — or refuses it
          // outright if the page's origin is not in CORS_ORIGINS.
          // Uncredentialed, so "*" is enough, same as /api/music/art.
          "access-control-allow-origin": "*",
        },
      });
    } catch (e) {
      // A signed URL that expired or was revoked: forget it so the next
      // request resolves afresh instead of replaying the failure for 5 min.
      resolvedStreams.delete(cacheKey);
      throw e;
    } finally {
      streamProxy.open--;
      streamProxy.finished++;
    }
  });

  fastify.get("/api/music/track", async (req, reply) => {
    const { id, quality, isrc, source, title, artist, duration } = req.query as {
      id?: string;
      quality?: string;
      isrc?: string;
      source?: SourcePriority;
      title?: string;
      artist?: string;
      duration?: string;
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
      );
      return reply.send(result);
    } catch (error) {
      return musicError(reply, 502, "All stream sources failed", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  });

  fastify.get("/api/music/manifest", async (req, reply) => {
    const { id, quality, isrc, artist, title, source, duration } =
      req.query as {
        id?: string;
        quality?: string;
        isrc?: string;
        artist?: string;
        title?: string;
        source?: SourcePriority;
        duration?: string;
      };

    if (!id) return musicError(reply, 400, "Missing required parameter: id");

    schedulePing();
    const sourceToUse = source || currentSourcePriority;

    // Nothing streams DASH any more — Tidal was the only source that did, and
    // it no longer plays. This endpoint stays because persisted queues from
    // before the change still carry isDash tracks that route here; it now just
    // resolves through the ordinary race and hands back a plain stream result.
    try {
      const result = await resolveStreamWithFallback(
        id,
        isrc,
        quality,
        sourceToUse,
        { title, artist, duration: duration ? Number(duration) : undefined },
      );

      reply.header("cache-control", "private, max-age=300");
      reply.header("access-control-allow-origin", "*");
      return reply.send(result);
    } catch (error) {
      return musicError(reply, 502, "All stream sources failed", {
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  });

}
