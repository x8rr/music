// YouTube Music as a search + audio source.
//
// This replaces the old `youtube` source, which went through yt-search (scraped
// watch pages, no album/ISRC metadata, and full of covers/sped-up reuploads)
// and then handed the player a raw googlevideo URL. Those URLs are IP- and
// session-bound, expire in minutes, and 403 the moment the player seeks past
// whatever the initial range covered.
//
// Instead we talk to YT Music's own innertube endpoint with the "songs only"
// search params, which returns exactly the catalogue tracks (title, artist,
// album, duration, square cover art) and none of the video noise — then let
// yt-dlp pull the audio down once and serve it off disk, so seeking is a plain
// local range request that can't expire mid-play.

import { promises as fsp, constants as fsConstants } from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import crypto from "node:crypto";
import { ConcurrencyGate } from "./concurrencyGate";
import { r2Get, r2PutFile } from "./r2";
import { LruCache } from "./lruCache";

// There is no built-in YouTube Music host: whoever runs this sets YTM_BASE to
// the origin to search and download from (https://music.youtube.com, or a
// mirror of it). Unset, the source is off. Read per call, not at load: another
// test file may import this module first.
export function ytmBase(): string | null {
  const raw = (process.env.YTM_BASE || "").trim().replace(/\/+$/, "");
  return /^https?:\/\//i.test(raw) ? raw : null;
}

export function ytmConfigured(): boolean {
  return ytmBase() !== null;
}

export class YtmNotConfiguredError extends Error {
  constructor() {
    super("YouTube Music is not configured: set YTM_BASE");
    this.name = "YtmNotConfiguredError";
  }
}

function requireYtmBase(): string {
  const base = ytmBase();
  if (!base) throw new YtmNotConfiguredError();
  return base;
}

// What music.youtube.com's own page sent on 2026-09-29. The version was two
// years stale and the UA a bare "Mozilla/5.0"; neither is proven to cause the
// 403s, but a request that looks like the web client costs nothing.
const INNERTUBE_CLIENT_VERSION = "1.20260927.17.00";
const BROWSER_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36";

function innertubeHeaders(base: string): Record<string, string> {
  const origin = new URL(base).origin;
  return {
    "Content-Type": "application/json",
    "X-Origin": origin,
    Origin: origin,
    Referer: `${origin}/`,
    "User-Agent": BROWSER_UA,
    "Accept-Language": "en-US,en;q=0.9",
    "X-Youtube-Client-Name": "67",
    "X-Youtube-Client-Version": INNERTUBE_CLIENT_VERSION,
  };
}

// Base64 protobuf for "filter: songs". Without it innertube returns the mixed
// top-results shelf (videos, playlists, artists) and the parse below has to
// guess which rows are actually tracks.
const SONGS_ONLY_PARAMS = "EgWKAQIIAWoKEAkQBRALEAoQBA==";

const SEARCH_TIMEOUT_MS = Number(process.env.YTM_SEARCH_TIMEOUT_MS) || 6000;

export interface YtmTrack {
  id: string;
  title: string;
  artist: string;
  album: string;
  duration: number | null;
  thumb: string;
}

/** The slice of innertube's musicResponsiveListItemRenderer we actually read.
 * Every level is optional because the response legitimately varies — a shelf
 * row without a watchEndpoint, a track with no musicThumbnailRenderer — and
 * the parse below is expected to reject those rather than throw. */
interface WatchEndpoint {
  watchEndpoint?: { videoId?: string };
}

interface TextRun {
  text: string;
  navigationEndpoint?: WatchEndpoint;
}

interface FlexColumn {
  musicResponsiveListItemFlexColumnRenderer?: { text?: { runs?: TextRun[] } };
}

interface ListItemRenderer {
  flexColumns?: FlexColumn[];
  thumbnail?: {
    musicThumbnailRenderer?: { thumbnail?: { thumbnails?: { url: string }[] } };
  };
  overlay?: {
    musicItemThumbnailOverlayRenderer?: {
      content?: {
        musicPlayButtonRenderer?: { playNavigationEndpoint?: WatchEndpoint };
      };
    };
  };
}

/** innertube nests the result rows under a shelf structure that changes shape
 * between response variants, so walk the whole tree and take every renderer
 * rather than trying to pin down a path. */
function collect(node: unknown, out: ListItemRenderer[]): void {
  if (!node || typeof node !== "object") return;
  if (Array.isArray(node)) {
    for (const child of node) collect(child, out);
    return;
  }
  const obj = node as Record<string, unknown>;
  if (obj.musicResponsiveListItemRenderer)
    out.push(obj.musicResponsiveListItemRenderer as ListItemRenderer);
  for (const value of Object.values(obj)) collect(value, out);
}

function parseDuration(value: string): number | null {
  const parts = value.split(":").map(Number);
  if (parts.some(Number.isNaN)) return null;
  if (parts.length === 2) return parts[0] * 60 + parts[1];
  return parts[0] * 3600 + parts[1] * 60 + parts[2];
}

function parseItem(item: ListItemRenderer): YtmTrack | null {
  const columns = item.flexColumns || [];
  const runs =
    columns[0]?.musicResponsiveListItemFlexColumnRenderer?.text?.runs;
  if (!runs?.[0]) return null;

  const first = runs[0];
  const id: string | undefined =
    first.navigationEndpoint?.watchEndpoint?.videoId ||
    item.overlay?.musicItemThumbnailOverlayRenderer?.content
      ?.musicPlayButtonRenderer?.playNavigationEndpoint?.watchEndpoint?.videoId;
  if (!id || !first.text) return null;

  // Second column reads "Artist • Album • 3:29", sometimes with a release year
  // and, for collaborations, several artist runs joined by " & ". Only " • " is
  // a field boundary, so group the runs on it first — splitting on every run
  // instead is what used to file the "&" between two artists as the album.
  const runsMeta: TextRun[] =
    columns[1]?.musicResponsiveListItemFlexColumnRenderer?.text?.runs || [];
  const fields: string[] = [];
  let field = "";
  for (const run of runsMeta) {
    if (run.text === " • ") {
      if (field.trim()) fields.push(field.trim());
      field = "";
    } else {
      field += run.text;
    }
  }
  if (field.trim()) fields.push(field.trim());

  let duration: number | null = null;
  const rest: string[] = [];
  for (const text of fields) {
    // h:mm:ss too: hour-long "songs" (lofi and sleep compilations) otherwise
    // kept the runtime as their album and passed as songs with no duration.
    if (duration === null && /^\d+(:\d{2}){1,2}$/.test(text))
      duration = parseDuration(text);
    else if (!/^\d{4}$/.test(text)) rest.push(text);
  }

  // Real YT Music covers come from googleusercontent and support a
  // "=w226-h226-l90" resize suffix; the i.ytimg.com fallback is a fixed static
  // file that 404s if the same suffix is appended, which is why tracks without
  // a musicThumbnailRenderer used to end up with no art at all. About a third
  // of responses come back in a "lean" layout whose thumbnails are i.ytimg.com
  // files too, and whose artists read "Band - Topic".
  const thumbs: { url: string }[] =
    item.thumbnail?.musicThumbnailRenderer?.thumbnail?.thumbnails || [];
  const best = thumbs.at(-1)?.url;
  const thumb = !best
    ? `https://i.ytimg.com/vi/${id}/mqdefault.jpg`
    : /\.(googleusercontent|ggpht)\.com\//.test(best)
      ? `${best.split("=")[0]}=w226-h226-l90`
      : best;

  return {
    id,
    title: first.text,
    artist: (rest[0] || "").replace(/ - Topic$/, ""),
    album: rest[1] || "",
    duration,
    thumb,
  };
}

// YouTube rate-limits this box's innertube searches under load (2026-09-29:
// ~600 403s and ~450 timeouts an hour), and a refused search also sinks every
// stream that has to look its video id up first. Most searches are the same
// few browse-row queries for every visitor, so successful ones are cached,
// which is what keeps the request rate under the limit. A search innertube
// still refuses is answered from the Invidious instance that already carries
// the downloads, for that request only: Invidious searches all of YouTube, so
// its answers are the fallback, never the norm. A 403 comes back fast; a
// timeout does not, so only a timeout pauses innertube, and briefly.
const searchCache = new LruCache<YtmTrack[]>("ytmSearch", 2000, 30 * 60_000);
const searchInFlight = new Map<string, Promise<YtmTrack[]>>();
const INNERTUBE_TIMEOUT_PAUSE_MS = 30_000;
let innertubeSkipUntil = 0;

export async function ytmSearch(
  query: string,
  limit: number,
): Promise<YtmTrack[]> {
  const q = query.trim();
  if (!q) return [];
  const key = `${q.toLowerCase()}|${limit}`;
  const hit = searchCache.get(key);
  if (hit) return hit;
  const running = searchInFlight.get(key);
  if (running) return running;
  const job = searchUncached(q, limit, key);
  searchInFlight.set(key, job);
  job.catch(() => {}).finally(() => searchInFlight.delete(key));
  return job;
}

async function searchUncached(q: string, limit: number, key: string): Promise<YtmTrack[]> {
  requireYtmBase();
  // Read per call, not at load: another test file may import this module first.
  const instance = process.env.YTM_INVIDIOUS_INSTANCE;
  if (instance && Date.now() < innertubeSkipUntil) return invidiousSearch(instance, q, limit);
  try {
    const tracks = await innertubeSearch(q, limit);
    searchCache.set(key, tracks);
    return tracks;
  } catch (err) {
    if (!instance) throw err;
    if (err instanceof Error && /abort|timed? ?out/i.test(err.message))
      innertubeSkipUntil = Date.now() + INNERTUBE_TIMEOUT_PAUSE_MS;
    return invidiousSearch(instance, q, limit);
  }
}

interface InvidiousVideo {
  type?: string;
  videoId?: string;
  title?: string;
  author?: string;
  authorVerified?: boolean;
  lengthSeconds?: number;
  liveNow?: boolean;
}

/** Invidious searches YouTube, not YouTube Music: music videos with intros,
 * lyric-channel reuploads, 8D and sped-up edits and hours-long "playlist"
 * compilations rank alongside the album track. Adding "audio" to the query puts
 * the official audio uploads first (checked: Espresso otherwise matched a remix
 * of the same length). One upload per song is kept, audio before video and the
 * artist's own channel before a reupload; a reupload counts only when its title
 * names the artist, so a channel name is never shown as the artist. Live streams are dropped: latest_version hangs on them until the
 * download times out. */
const NOT_A_SONG = /\b(mix|mixtape|playlist|compilation|full album|non-?stop|hours?|hrs?|megamix)\b/i;
const MUSIC_VIDEO = /\b(official (music )?video|music video|video oficial|video clip|official mv|m\/v)\b/i;
const SONG_MIN_S = 45;
const SONG_MAX_S = 10 * 60;
// Catalog tracks run long more often than videos (prog, DJ edits, the
// 10-minute All Too Well), but not for an hour.
const CATALOG_MAX_S = 20 * 60;
async function invidiousSearch(instance: string, q: string, limit: number, suffix = " audio"): Promise<YtmTrack[]> {
  const response = await fetch(
    `https://${instance}/api/v1/search?type=video&q=${encodeURIComponent(`${q}${suffix}`)}`,
    { signal: AbortSignal.timeout(SEARCH_TIMEOUT_MS) },
  );
  if (!response.ok) {
    await response.body?.cancel().catch(() => {});
    throw new Error(`invidious search HTTP ${response.status}`);
  }
  const rows = (await response.json()) as InvidiousVideo[];
  const ranked: [number, YtmTrack][] = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    if (row.type !== "video" || !row.videoId || !row.title || row.liveNow) continue;
    const length = row.lengthSeconds ?? 0;
    if (length < SONG_MIN_S || length > SONG_MAX_S || NOT_A_SONG.test(row.title)) continue;
    const video = splitVideoTitle(row.title, row.author ?? "");
    const track = {
      id: row.videoId,
      title: video.title,
      artist: video.artist,
      album: "",
      duration: length,
      thumb: `https://i.ytimg.com/vi/${row.videoId}/mqdefault.jpg`,
    };
    // A "- Topic" channel is generated by YouTube from the label's delivery,
    // so its name is the artist even though it is never marked verified.
    const own = video.fromChannel && (video.named || row.authorVerified || / - Topic$/.test(row.author ?? ""));
    // A bare title from an unverified channel would show the channel as the artist.
    if (!own && !video.named) continue;
    // Music videos carry intros and skits, so any audio upload of the song
    // beats one, and among each kind the artist's own upload beats a reupload.
    ranked.push([(MUSIC_VIDEO.test(row.title) ? 2 : 0) + (own ? 0 : 1), track]);
  }
  ranked.sort((a, b) => a[0] - b[0]);
  return tidy(ranked.map(([, t]) => t), q, SONG_MAX_S).slice(0, limit);
}

const norm = (s: string) =>
  s.normalize("NFKD").replace(/\p{M}|['’]/gu, "").toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
const compact = (s: string) => norm(s).replace(/ /g, "");

const ARTIST_SPLIT = /\s*(?:,|&|\bx\b|\bfeat\.?|\bft\.?|\bwith\b)\s*/i;

/** "Queen Official", "Official Arctic Monkeys", "TaylorSwiftVEVO" and
 * "Drake - Topic" are all the artist's own channel. */
function channelIsArtist(channel: string, artist: string): boolean {
  const c = compact(channel.replace(/ - Topic$/, ""));
  return artist
    .split(ARTIST_SPLIT)
    .map(compact)
    .some((a) => a !== "" && (c === a || c === `${a}vevo` || c === `official${a}` || c === `${a}official`));
}

// Tags that describe the upload, not the song: bracketed anywhere, or bare at
// the end ("Creep HQ", "Numb Lyrics", "... / FLAC").
const UPLOAD_TAG =
  /\s*[([][^)\]]*\b(official|audio|video|lyrics?|letra|visuali[sz]er|hq|hd|4k|explicit|mv)\b[^)\]]*[)\]]/gi;
const TRAILING_TAGS =
  /(\s*[/|]?\s*\b(hq|hd|flac|mp3|lyrics?|audio|dolby atmos|\d+\s?bit|hi-?res|official (music )?(video|audio|mv)))+\s*$/i;

/** Artist and song from a video title. "Band - Song (Official Audio)" names
 * the artist; "Song" alone takes it from the channel. `fromChannel` is true
 * when that artist is the uploader, `named` when the title spelled it out. */
export function splitVideoTitle(
  raw: string,
  channel: string,
): { artist: string; title: string; fromChannel: boolean; named: boolean } {
  const cleaned =
    raw
      .split(" | ")[0]
      .replace(UPLOAD_TAG, "")
      .replace(/\s+#\S+/g, "")
      .replace(TRAILING_TAGS, "")
      .trim() || raw;
  const dash = cleaned.match(/^(.+?)\s+[-–—]\s+(.+)$/);
  if (dash) {
    let [, artist, title] = dash;
    // "Numb – Linkin Park" from the Linkin Park channel.
    if (!channelIsArtist(channel, artist) && channelIsArtist(channel, title)) [artist, title] = [title, artist];
    return { artist: artist.trim(), title: title.trim(), fromChannel: channelIsArtist(channel, artist), named: true };
  }
  const artist = channel.replace(/ - Topic$/, "").replace(/^Official\s+|\s+Official$|VEVO$/gi, "").trim();
  return { artist, title: cleaned, fromChannel: true, named: false };
}

// Copies of a song that aren't the song. The second list only counts inside
// brackets, since "Live Forever" and "Clean" are real titles.
const ALTERED =
  /\b(\d{1,2}d|sped[\s-]?up|speed[\s-]?up|slowed|reverb|nightcore|\d+\s?hz|karaoke|bass[\s-]?boost(ed)?|dance practice|mirrored|isolated vocals|reaction|mashup)\b/gi;
const ALTERED_IN_BRACKETS = /\b(live|cover|clean|edit|remix|extended|instrumental|loop)\b/gi;

/** True for "Song (8D Audio)" or "Song (Live)", unless the search asked for it. */
export function isAlteredCopy(title: string, query: string): boolean {
  const q = query.toLowerCase();
  const brackets = (title.match(/[([][^)\]]*[)\]]/g) ?? []).join(" ");
  const hits = [...(title.match(ALTERED) ?? []), ...(brackets.match(ALTERED_IN_BRACKETS) ?? [])];
  return hits.some((w) => !q.includes(w.toLowerCase()));
}

/** Drops altered copies and over-long runtimes, and keeps the first of each
 * artist + title, so a song shows once however many uploads of it there are.
 * The key ignores which side is which: reuploads title it "Song - Artist" as
 * often as the other way round. */
function tidy(tracks: YtmTrack[], q: string, maxSeconds: number): YtmTrack[] {
  const seen = new Set<string>();
  return tracks.filter((t) => {
    if ((t.duration ?? 0) > maxSeconds || isAlteredCopy(`${t.artist} ${t.title}`, q)) return false;
    const key = [compact(t.artist.split(/,|&/)[0]), compact(t.title)].sort().join("|");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function innertubeSearch(q: string, limit: number): Promise<YtmTrack[]> {
  const base = requireYtmBase();
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), SEARCH_TIMEOUT_MS);
  let response: Response;
  try {
    response = await fetch(`${base}/youtubei/v1/search?prettyPrint=false`, {
      method: "POST",
      headers: innertubeHeaders(base),
      body: JSON.stringify({
        context: {
          client: {
            clientName: "WEB_REMIX",
            clientVersion: INNERTUBE_CLIENT_VERSION,
            hl: "en",
            gl: "US",
            userAgent: BROWSER_UA,
            platform: "DESKTOP",
          },
        },
        query: q,
        params: SONGS_ONLY_PARAMS,
      }),
      signal: ctrl.signal,
    });
  } finally {
    clearTimeout(timer);
  }

  if (!response.ok) {
    await response.body?.cancel().catch(() => {});
    throw new Error(`innertube HTTP ${response.status}`);
  }

  const json = await response.json();
  const items: ListItemRenderer[] = [];
  collect(json, items);
  const tracks = items.map(parseItem).filter((t): t is YtmTrack => t !== null);
  return tidy(tracks, q, CATALOG_MAX_S).slice(0, limit);
}

// More than this far out and it is a different cut of the song, not the track
// we were asked for.
const RUNTIME_WINDOW_S = 8;

/** The song a title names, without what brackets and a dash suffix add:
 * "(2011 Remaster)", "(from "Silksong")", " - Single Version". */
const songOf = (title: string) =>
  norm(title.replace(/[([][^)\]]*[)\]]/g, " ").split(/\s[-–—]\s/)[0]) || norm(title);

/** Words that mark a different recording of the same song. A title has to carry
 * the same ones as the request: "Song (Live)" is not "Song", and the song
 * is not "Song - Remix" however well the runtime lines up. */
const VERSION_WORDS =
  /\b(live|remix|rmx|acoustic|instrumental|cover|karaoke|demo|a ?cappella|acapella|sped[\s-]?up|slowed|reverb|nightcore|rework|bootleg|mashup|8d|piano version|stripped|unplugged)\b/gi;

export function sameVersion(candidate: string, wanted: string): boolean {
  const words = (t: string) => [...new Set((t.match(VERSION_WORDS) ?? []).map((w) => w.toLowerCase()))].sort().join(",");
  return words(candidate) === words(wanted);
}

function sameArtist(candidate: string, artist: string): boolean {
  const have = candidate.split(ARTIST_SPLIT).map(compact);
  return artist.split(ARTIST_SPLIT).map(compact).some((a) => a !== "" && have.includes(a));
}

/** A YouTube upload of the recording itself, for a track YT Music's catalogue
 * only has as other people's versions. The upload has to name both the song
 * and the artist and run the same length: a cover names the song and keeps
 * the runtime, but is credited to whoever played it. */
async function findUpload(artist: string, title: string, durationSec?: number): Promise<string | null> {
  const instance = process.env.YTM_INVIDIOUS_INSTANCE;
  if (!instance) return null;
  const song = ` ${songOf(title)} `;
  const names = artist.split(ARTIST_SPLIT).map(norm).filter(Boolean);
  // The plain "artist title audio" search buries an artist's own Topic upload
  // under reuploads and covers (The Arch-Illager, 2026-10-06), and the artist
  // string from Tidal lists every credited name. Quoting the exact title and
  // one artist at a time finds it; the loose search stays as the last try.
  const exact = title.replace(/[([][^)\]]*[)\]]/g, " ").split(/\s[-–—]\s/)[0].replace(/"/g, "").trim() || title;
  const queries: [string, string][] = [
    ...artist
      .split(ARTIST_SPLIT)
      .map((n) => n.trim().replace(/"/g, ""))
      .filter(Boolean)
      .slice(0, 3)
      .map((n): [string, string] => [`"${exact}" "${n}"`, ""]),
    [`${artist} ${title}`.trim(), " audio"],
  ];
  const found = await Promise.all(queries.map(([q, suffix]) => invidiousSearch(instance, q, 10, suffix).catch(() => [])));
  const seen = new Set<string>();
  const uploads = found.flat().filter((t) => !seen.has(t.id) && seen.add(t.id));
  const match = uploads.find((t) => {
    if (durationSec && Math.abs((t.duration ?? 0) - durationSec) > RUNTIME_WINDOW_S) return false;
    if (!sameVersion(t.title, title)) return false;
    const said = ` ${norm(`${t.artist} ${t.title}`)} `;
    return said.includes(song) && names.some((n) => said.includes(` ${n} `));
  });
  return match?.id ?? null;
}

/**
 * Best match for a track we already know by name. Used when the player is
 * streaming a Tidal-sourced result through ytm: search once for
 * "artist title", then prefer the candidate whose runtime lines up, since
 * YT Music readily returns a remix or a live cut under the same title.
 */
export async function ytmResolveVideoId(
  artist: string,
  title: string,
  durationSec?: number,
): Promise<string | null> {
  const results = await ytmSearch(`${artist} ${title}`.trim(), 10);
  const inWindow = durationSec
    ? results.filter((t) => t.duration !== null && Math.abs(t.duration - durationSec) <= RUNTIME_WINDOW_S)
    : results;
  const wanted = norm(title);

  // The artist has to match as well as the runtime. A soundtrack that is not
  // on YT Music still returns "songs" for its titles, all of them covers, and
  // a piano cover made to the original's length used to win on runtime alone
  // (Deep Docks, 2026-10-03).
  const theirs = inWindow.filter(
    (t) => sameArtist(t.artist, artist) && songOf(t.title) === songOf(title) && sameVersion(t.title, title),
  );
  const own = theirs.find((t) => norm(t.title) === wanted) ?? theirs[0];
  if (own) return own.id;

  const upload = await findUpload(artist, title, durationSec);
  if (upload) return upload;

  // Same artist, same song, same version, but the saved length is off by more
  // than a re-encode: a playlist entry saved from Qobuz carries Qobuz's
  // runtime, and the same recording often differs by ten seconds or so
  // (Cellar, 2026-10-06). The runtime only ranks these, and the bound keeps
  // a loop or an extended cut out.
  if (durationSec) {
    const bound = Math.max(RUNTIME_WINDOW_S, Math.min(60, durationSec * 0.2));
    const near = results
      .filter(
        (t) =>
          t.duration !== null &&
          Math.abs(t.duration - durationSec) <= bound &&
          sameArtist(t.artist, artist) &&
          songOf(t.title) === songOf(title) &&
          sameVersion(t.title, title),
      )
      .sort((a, b) => Math.abs(a.duration! - durationSec) - Math.abs(b.duration! - durationSec));
    if (near[0]) return near[0].id;
  }

  // No match is the answer. A last "closest result" used to play whatever
  // ranked first when nothing credited the artist, which is how a different
  // artist's "Cellar" of about the same length played in place of Peter
  // Hont's. A track that fails to play is a smaller mistake than a wrong one.
  return null;
}

// --- yt-dlp audio cache -----------------------------------------------------

const CACHE_DIR =
  process.env.YTM_CACHE_DIR ||
  path.join(process.cwd(), "data", "ytm-cache");

// yt-dlp can sit for minutes when YouTube throws a bot check at it, and this
// one downloads the whole track rather than just resolving a URL. Bound it so
// a wedged download loses the stream race instead of holding a play open.
const DOWNLOAD_TIMEOUT_MS = Number(process.env.YTM_DOWNLOAD_TIMEOUT_MS) || 45000;

// Which innertube clients to ask for stream URLs, tried **one at a time**.
// Only used when no Invidious instance is set (see ytDlpAttempts).
//
// This has to be sequential, not a comma list handed to yt-dlp in one go. With
// a list, yt-dlp merges the formats from every client and picks one by the -f
// expression — so a client whose URLs 403 can win the selection and the
// download dies with "unable to download video data: HTTP Error 403". The 403
// happens after the format is chosen, and yt-dlp will not go back and try a
// different client, so the only reliable fallback is our own: run it again,
// pinned to exactly one client, until a run actually produces a file.
//
// Order is "most likely to serve bytes" first. Which clients work changes as
// YouTube pushes back; set YTM_PLAYER_CLIENTS (comma-separated) to re-order or
// add one without editing code.
const PLAYER_CLIENTS: string[] = (
  process.env.YTM_PLAYER_CLIENTS ||
  "web_embedded,default,tv_embedded,web_safari,mweb,ios"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// Most of a cold track's wall time is yt-dlp fetching YouTube's player and
// solving its JS challenge, not the download. yt-dlp caches that work — but in
// $XDG_CACHE_HOME/yt-dlp, and a service started by pm2 may have no writable
// HOME, in which case it silently re-does the whole thing on every track. Pin
// the cache next to the audio cache so it is writable and survives restarts.
const YT_DLP_CACHE_DIR =
  process.env.YTM_YTDLP_CACHE_DIR ||
  path.join(process.cwd(), "data", "ytm-cache", ".yt-dlp");

// Exactly 11 characters, not a loose length range: Tidal track ids are 8-9
// digits and would otherwise pass, which sent a Tidal id straight to yt-dlp
// ("Unsupported URL") instead of looking the track up by name.
const VIDEO_ID_RE = /^[A-Za-z0-9_-]{11}$/;

export function isYtmVideoId(id: string): boolean {
  return VIDEO_ID_RE.test(id);
}

/** An explicit path first, then the usual install locations. PATH itself is
 * not searched. */
let ytDlpPathPromise: Promise<string | null> | null = null;
export function resolveYtDlpPath(): Promise<string | null> {
  if (!ytDlpPathPromise) {
    ytDlpPathPromise = (async () => {
      const candidates = [
        process.env.YT_DLP_PATH,
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

// Filenames by video id, built from one readdir at first use and kept up to
// date by the rename-in below. Looking up a track used to list the whole
// directory (thousands of entries) on every play attempt and every seek.
// A file the sweep removes drops out on the stat that follows a hit.
let cacheIndex: Promise<Map<string, string>> | null = null;
function loadCacheIndex(): Promise<Map<string, string>> {
  if (!cacheIndex) {
    cacheIndex = fsp
      .readdir(CACHE_DIR)
      .then((names) => {
        const map = new Map<string, string>();
        for (const name of names) {
          if (name.includes(".tmp.")) continue;
          const dot = name.indexOf(".");
          if (dot > 0) map.set(name.slice(0, dot), name);
        }
        return map;
      })
      .catch(() => new Map<string, string>());
  }
  return cacheIndex;
}

/** A completed cache entry for `id`, or null. Partial downloads are written
 * under a `.tmp.` name and renamed into place, so anything indexed here is a
 * whole file. */
async function cachedFile(id: string): Promise<string | null> {
  const index = await loadCacheIndex();
  const name = index.get(id);
  if (!name) return null;
  const file = path.join(CACHE_DIR, name);
  try {
    await fsp.access(file);
    return file;
  } catch {
    index.delete(id);
    return null;
  }
}

// One download per video id, shared by every concurrent request. Without this,
// two listeners starting the same track (or the stream race probing while the
// player also asks) spawn two yt-dlp processes fighting over the same output.
const pending = new Map<string, Promise<string>>();

// Each yt-dlp run is a ~75MB Python process and the stream race starts one
// per play attempt, so with nothing capping it a busy hour meant dozens at
// once — several GB charged to the box rather than to this process, which is
// what got the backend OOM-killed. Cap the parallelism, and bound the queue so
// a backlog fails straight into the other sources instead of piling up.
const MAX_CONCURRENT_DOWNLOADS =
  Number(process.env.YTM_MAX_CONCURRENT_DOWNLOADS) || 3;
const MAX_QUEUED_DOWNLOADS = 12;
const downloadGate = new ConcurrencyGate(
  MAX_CONCURRENT_DOWNLOADS,
  MAX_QUEUED_DOWNLOADS,
);

// YouTube pushing back looks like every client failing with a bot check or a
// 403. After a few tracks in a row like that the source is parked: each further
// attempt would spend up to six 45-second runs learning the same thing while
// holding a download slot.
const BLOCKED_STREAK_TO_PARK = 3;
const PARK_MS = Number(process.env.YTM_PARK_MS) || 5 * 60_000;
let blockedStreak = 0;
let parkedUntil = 0;

// A video that just failed on every client will not succeed a moment later,
// but the race asks for it again on the very next play of the same track.
const FAILED_TTL_MS = 10 * 60_000;
const FAILED_MAX = 500;
const recentFailures = new Map<string, number>();

function looksBlocked(failures: string[]): boolean {
  return failures.some((f) =>
    /HTTP Error 403|HTTP Error 429|Sign in to confirm|not a bot/i.test(f),
  );
}

function noteFailure(id: string, failures: string[]): void {
  recentFailures.set(id, Date.now());
  if (recentFailures.size > FAILED_MAX) {
    const cutoff = Date.now() - FAILED_TTL_MS;
    for (const [k, at] of recentFailures) if (at < cutoff) recentFailures.delete(k);
    if (recentFailures.size > FAILED_MAX) recentFailures.clear();
  }
  if (!looksBlocked(failures)) return;
  blockedStreak++;
  if (blockedStreak >= BLOCKED_STREAK_TO_PARK && Date.now() >= parkedUntil) {
    parkedUntil = Date.now() + PARK_MS;
    console.error(
      `[ytm] YouTube is refusing downloads (${blockedStreak} tracks in a row); parking the source for ${Math.round(PARK_MS / 1000)}s`,
    );
  }
}

/** Diagnostic sizes for /api/system/memory. */
export function ytmStats() {
  return {
    ...downloadGate.stats(),
    parkedForSec: Math.max(0, Math.ceil((parkedUntil - Date.now()) / 1000)),
    recentFailures: recentFailures.size,
  };
}

function runYtDlp(binary: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args);
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`yt-dlp timed out after ${DOWNLOAD_TIMEOUT_MS}ms`));
    }, DOWNLOAD_TIMEOUT_MS);
    let stderr = "";
    child.stderr?.on("data", (chunk) => {
      // Keep only the tail: a bot-check failure can emit a lot of output and
      // all we surface is the reason the source lost the race.
      stderr = (stderr + String(chunk)).slice(-2000);
    });
    child.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code === 0) resolve();
      else
        reject(
          new Error(`yt-dlp exited ${code}${stderr ? `: ${stderr.trim()}` : ""}`),
        );
    });
  });
}

/** Remove any partial output from a failed attempt. yt-dlp leaves `.part`
 * files (and the target itself) behind when it is killed mid-download. */
async function cleanupTemp(id: string, tag: string): Promise<void> {
  try {
    const names = await fsp.readdir(CACHE_DIR);
    await Promise.all(
      names
        .filter((x) => x.startsWith(`${id}.${tag}.tmp.`))
        .map((x) => fsp.rm(path.join(CACHE_DIR, x), { force: true })),
    );
  } catch {
    // Best effort — a stale temp file is harmless, cachedFile ignores them.
  }
}

/** Where every downloaded track is kept for good; the disk holds the recently
 * played ones. An object so tests can swap in a Map. */
export const ytmStore = { get: r2Get, putFile: r2PutFile };

/** Pulls a stored track back onto the disk, or null to fall through to yt-dlp. */
async function fromStore(id: string): Promise<string | null> {
  const res = await ytmStore.get(`ytm/${id}`);
  if (!res) return null;
  const ext = res.headers.get("content-type") === "audio/mp4" ? ".m4a" : ".webm";
  const tmp = path.join(CACHE_DIR, `${id}.${crypto.randomBytes(4).toString("hex")}.tmp${ext}`);
  const final = path.join(CACHE_DIR, `${id}${ext}`);
  try {
    await fsp.mkdir(CACHE_DIR, { recursive: true });
    // Not Bun.write(tmp, res): with an abort signal on the fetch (r2Get's
    // header timeout) it never resolves, and every evicted track hung.
    const sink = Bun.file(tmp).writer();
    let written = 0;
    for await (const chunk of res.body!) {
      sink.write(chunk);
      written += chunk.byteLength;
    }
    await sink.end();
    // A cut-off body would otherwise be cached, and served, as the whole track.
    const expected = Number(res.headers.get("content-length"));
    if (expected && written !== expected) throw new Error(`got ${written} of ${expected} bytes`);
    await fsp.rename(tmp, final);
  } catch (err) {
    await fsp.rm(tmp, { force: true }).catch(() => {});
    console.warn(`[ytm] pulling ${id} from R2 failed:`, err instanceof Error ? err.message : err);
    return null;
  }
  (await loadCacheIndex()).set(id, path.basename(final));
  return final;
}

function download(id: string): Promise<string> {
  const existing = pending.get(id);
  if (existing) return existing;
  // R2 before yt-dlp: a track downloaded once still plays while YouTube is
  // refusing us, and a pull never takes one of the yt-dlp slots.
  const job = fromStore(id).then((file) => file ?? downloadWithYtDlp(id));
  pending.set(id, job);
  job.catch(() => {}).finally(() => pending.delete(id));
  return job;
}

/** yt-dlp runs to try in order for `id`, each as [label, args ending in the URL].
 * The env is read per call, not at load: another test file may import this
 * module first. */
function ytDlpAttempts(id: string): [string, string[]][] {
  const base = requireYtmBase();
  const instance = process.env.YTM_INVIDIOUS_INSTANCE;
  if (!instance)
    return PLAYER_CLIENTS.map((client) => [
      client,
      [
        "--extractor-args",
        `youtube:player_client=${client}`,
        `${base}/watch?v=${id}`,
      ],
    ]);
  // YouTube bot-checks this box on every client, so everything goes through
  // the instance. googlevideo throttles an unranged GET to ~30 KB/s; a chunk
  // size makes yt-dlp ask in ranges, which come back at full speed.
  const chunked = ["--http-chunk-size", "10M"];
  // latest_version first: with local=true the instance streams the audio
  // itself, while the plugin hands yt-dlp googlevideo URLs that this box then
  // fetches directly, and YouTube throttles us there. Measured on prod the same
  // 4 MB track took 5.9 s this way and 27.4 s through the plugin, and while the
  // instance had /api/v1/videos disabled that didn't show, because the plugin
  // failed in ~3 s and fell through to here. Once the API came back every
  // download went the slow way and the download queue overflowed.
  const latest = (itag: number): [string, string[]] => [
    `latest_version ${itag}`,
    [...chunked, `https://${instance}/latest_version?id=${id}&itag=${itag}&local=true`],
  ];
  return [
    latest(140),
    latest(251),
    [
      "invidious",
      [
        ...chunked,
        // The plugin retries a 500 five times, 5s apart, by default, and a 500
        // is how Invidious answers an unavailable video: ~30s of a download slot.
        "--extractor-args",
        `invidious:preferred_instance=${instance};max_retries=1`,
        `invidious:${id}`,
      ],
    ],
  ];
}

function downloadWithYtDlp(id: string): Promise<string> {
  if (Date.now() < parkedUntil)
    return Promise.reject(new Error("YouTube is refusing downloads, retrying later"));
  const failedAt = recentFailures.get(id);
  if (failedAt !== undefined) {
    if (Date.now() - failedAt < FAILED_TTL_MS)
      return Promise.reject(new Error("yt-dlp failed for this track recently"));
    recentFailures.delete(id);
  }

  return downloadGate.run(async () => {
    const binary = await resolveYtDlpPath();
    if (!binary)
      throw new Error(
        "yt-dlp not found. Install it, or set YT_DLP_PATH to its location.",
      );

    await fsp.mkdir(CACHE_DIR, { recursive: true });

    const failures: string[] = [];
    for (const [client, extra] of ytDlpAttempts(id)) {
      // A random tag per attempt keeps two runs for the same id — a retry with
      // the next client, or a concurrent run after a restart, where `pending`
      // is empty — from colliding on the same temp file.
      const tag = crypto.randomBytes(4).toString("hex");
      const target = path.join(CACHE_DIR, `${id}.${tag}.tmp.%(ext)s`);

      try {
        await runYtDlp(binary, [
          "-f",
          "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
          "-o",
          target,
          "--cache-dir",
          YT_DLP_CACHE_DIR,
          "--no-warnings",
          "--quiet",
          ...extra,
        ]);
      } catch (e) {
        failures.push(`${client}: ${e instanceof Error ? e.message : e}`);
        // A killed run can still leave a partial file behind; drop it so the
        // cache scan can't mistake it for a finished download.
        await cleanupTemp(id, tag);
        continue;
      }

      const name = (await fsp.readdir(CACHE_DIR)).find((x) =>
        x.startsWith(`${id}.${tag}.tmp.`),
      );
      if (!name) {
        // Exit 0 with nothing written: treat as a failure of this client
        // rather than of the track.
        failures.push(`${client}: exited 0 but produced no file`);
        continue;
      }

      const final = path.join(CACHE_DIR, `${id}${path.extname(name)}`);
      await fsp.rename(path.join(CACHE_DIR, name), final);
      (await loadCacheIndex()).set(id, path.basename(final));
      // A failed copy only means the next eviction costs a fresh download.
      ytmStore.putFile(`ytm/${id}`, final, ytmMimeType(final)).catch((err) =>
        console.warn(`[ytm] copying ${id} to R2 failed:`, err instanceof Error ? err.message : err),
      );
      blockedStreak = 0;
      return final;
    }

    noteFailure(id, failures);
    throw new Error(`all yt-dlp clients failed — ${failures.join(" | ")}`);
  });
}

export function ytmMimeType(file: string): string {
  return path.extname(file) === ".m4a" ? "audio/mp4" : "audio/webm";
}

/**
 * Path to a fully downloaded audio file for `id`, fetching it if we don't have
 * it yet. Resolving through this (rather than handing back a googlevideo URL)
 * is what makes the /api/music/ytm/audio route a plain local file read.
 */
export async function ytmAudioFile(id: string): Promise<string> {
  if (!isYtmVideoId(id)) throw new Error("invalid YouTube Music video id");
  requireYtmBase();
  return (await cachedFile(id)) ?? (await download(id));
}

/** A cache hit, without triggering a download. */
export function ytmCachedFile(id: string): Promise<string | null> {
  if (!isYtmVideoId(id)) return Promise.resolve(null);
  return cachedFile(id);
}

export function ytmCacheDir(): string {
  return CACHE_DIR;
}
