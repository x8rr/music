import { afterAll, beforeAll, expect, test } from "bun:test";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";

let dir: string;
let mod: typeof import("./ytm");

beforeAll(async () => {
  dir = await fsp.mkdtemp(path.join(os.tmpdir(), "ytm-"));
  process.env.YTM_CACHE_DIR = dir;
  process.env.YTM_BASE = "https://music.youtube.com";
  mod = await import("./ytm");
});
afterAll(async () => {
  await fsp.rm(dir, { recursive: true, force: true });
});

test("a track kept in the store is pulled to disk instead of downloaded", async () => {
  const id = "dQw4w9WgXcQ";
  const { get } = mod.ytmStore;
  mod.ytmStore.get = async (k) =>
    k === `ytm/${id}`
      ? new Response("audiobytes", { headers: { "content-type": "audio/mp4", "content-length": "10" } })
      : null;
  // Another test file may have imported ytm first and fixed the cache dir, so
  // go by what the module uses and clean up after.
  const file = path.join(mod.ytmCacheDir(), `${id}.m4a`);
  try {
    expect(await mod.ytmAudioFile(id)).toBe(file);
    expect(await fsp.readFile(file, "utf8")).toBe("audiobytes");
    expect(await mod.ytmCachedFile(id)).toBe(file);
    expect((await fsp.readdir(mod.ytmCacheDir())).filter((n) => n.startsWith(`${id}.`) && n.includes(".tmp"))).toEqual([]);
  } finally {
    mod.ytmStore.get = get;
    await fsp.rm(file, { force: true });
  }
});

test("with an Invidious instance set, downloads go through latest_version first, then the plugin", async () => {
  const id = "invidiousT1";
  const log = path.join(dir, "argv.log");
  const fake = path.join(dir, "fake-yt-dlp");
  // Logs the URL (last arg); only the plugin run "downloads", so every attempt shows.
  await fsp.writeFile(
    fake,
    `#!/bin/sh
for a; do url=$a; done
echo "$url" >> "$FAKE_YTDLP_LOG"
case "$url" in invidious:*) ;; *) echo "ERROR: HTTP Error 403: Forbidden" >&2; exit 1;; esac
while [ $# -gt 0 ]; do [ "$1" = -o ] && out=$2; shift; done
printf audio > "$(echo "$out" | sed 's/%(ext)s/m4a/')"
`,
    { mode: 0o755 },
  );
  process.env.YT_DLP_PATH = fake;
  process.env.YTM_INVIDIOUS_INSTANCE = "inv.test";
  process.env.FAKE_YTDLP_LOG = log;
  const { get, putFile } = mod.ytmStore;
  const puts: string[][] = [];
  mod.ytmStore.get = async () => null;
  mod.ytmStore.putFile = async (key, _file, type) => void puts.push([key, type]);
  const file = path.join(mod.ytmCacheDir(), `${id}.m4a`);
  try {
    expect(await mod.ytmAudioFile(id)).toBe(file);
    expect((await fsp.readFile(log, "utf8")).trim().split("\n")).toEqual([
      `https://inv.test/latest_version?id=${id}&itag=140&local=true`,
      `https://inv.test/latest_version?id=${id}&itag=251&local=true`,
      `invidious:${id}`,
    ]);
    expect(puts).toEqual([[`ytm/${id}`, "audio/mp4"]]);
    expect((await fsp.readdir(mod.ytmCacheDir())).filter((n) => n.startsWith(`${id}.`) && n.includes(".tmp"))).toEqual([]);
  } finally {
    for (const k of ["YT_DLP_PATH", "YTM_INVIDIOUS_INSTANCE", "FAKE_YTDLP_LOG"]) delete process.env[k];
    mod.ytmStore.get = get;
    mod.ytmStore.putFile = putFile;
    await fsp.rm(file, { force: true });
  }
});

// Minimal innertube songs-shelf row: title run with the video id, then
// "Artist • Album • m:ss".
const innertubeRow = (id: string, title: string, artist: string, time: string) => ({
  musicResponsiveListItemRenderer: {
    flexColumns: [
      { musicResponsiveListItemFlexColumnRenderer: { text: { runs: [{ text: title, navigationEndpoint: { watchEndpoint: { videoId: id } } }] } } },
      { musicResponsiveListItemFlexColumnRenderer: { text: { runs: [{ text: artist }, { text: " • " }, { text: "Album" }, { text: " • " }, { text: time }] } } },
    ],
  },
});

test("innertube songs are cached; a refusal falls back to song-length Invidious results for that request only", async () => {
  const realFetch = globalThis.fetch;
  const hits: string[] = [];
  let refuse = false;
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = String(input);
    if (url.startsWith("https://music.youtube.com")) {
      hits.push("innertube");
      if (refuse) return new Response("no", { status: 403 });
      return Response.json({ contents: [innertubeRow("vidSong0001", "Song", "Band", "3:32")] });
    }
    hits.push(url);
    return Response.json([
      { type: "video", videoId: "vidPlaylist", title: "Indie Compilation (2-hour playlist)", author: "Chan", lengthSeconds: 7200 },
      { type: "video", videoId: "vidPartyMix", title: "Band Party Mix", author: "Chan", lengthSeconds: 300 },
      { type: "video", videoId: "vidLiveNow1", title: "Song live 24/7", author: "Band", lengthSeconds: 0, liveNow: true },
      { type: "channel", author: "Band" },
      { type: "video", videoId: "vidOfficial", title: "Band - Song (Official Audio)", author: "Band", lengthSeconds: 212 },
    ]);
  }) as typeof fetch;
  process.env.YTM_INVIDIOUS_INSTANCE = "inv.test";
  try {
    const songs = await mod.ytmSearch("cache me", 5);
    expect(songs.map((t) => [t.id, t.title, t.artist, t.duration])).toEqual([["vidSong0001", "Song", "Band", 212]]);
    // The same query again is served from the cache.
    expect(await mod.ytmSearch("Cache Me", 5)).toEqual(songs);
    expect(hits).toEqual(["innertube"]);

    hits.length = 0;
    refuse = true;
    const fallback = await mod.ytmSearch("band song", 5);
    expect(fallback.map((t) => t.id)).toEqual(["vidOfficial"]);
    expect(fallback[0]).toMatchObject({ title: "Song", artist: "Band", duration: 212 });
    expect(hits).toEqual(["innertube", "https://inv.test/api/v1/search?type=video&q=band%20song%20audio"]);

    // A fallback answer isn't cached and a 403 doesn't pause innertube: the
    // next search asks it again.
    hits.length = 0;
    refuse = false;
    expect(await mod.ytmResolveVideoId("Band", "Song", 213)).toBe("vidSong0001");
    expect(hits).toEqual(["innertube"]);
  } finally {
    globalThis.fetch = realFetch;
    delete process.env.YTM_INVIDIOUS_INSTANCE;
  }
});

test("video titles split into artist and song, whichever way round", () => {
  const split = (title: string, channel: string) => {
    const v = mod.splitVideoTitle(title, channel);
    return [v.artist, v.title, v.fromChannel];
  };
  expect(split("Sabrina Carpenter - Espresso (Official Audio)", "Sabrina Carpenter")).toEqual(["Sabrina Carpenter", "Espresso", true]);
  expect(split("Not Like Us", "Kendrick Lamar")).toEqual(["Kendrick Lamar", "Not Like Us", true]);
  expect(split("Numb (Official Music Video) [4K UPGRADE] – Linkin Park", "Linkin Park")).toEqual(["Linkin Park", "Numb", true]);
  expect(split("Bohemian Rhapsody", "Queen Official")).toEqual(["Queen", "Bohemian Rhapsody", true]);
  expect(split("Do I Wanna Know?", "Official Arctic Monkeys")).toEqual(["Arctic Monkeys", "Do I Wanna Know?", true]);
  expect(split("BAD BUNNY - NUEVAYoL (Video Oficial) | DeBÍ TiRAR MáS FOToS", "Bad Bunny")).toEqual(["BAD BUNNY", "NUEVAYoL", true]);
  expect(split("Radiohead - Creep HQ (Audio)", "MusickLife")).toEqual(["Radiohead", "Creep", false]);
  expect(split("Sabrina Carpenter - Espresso (Lyrics)", "7clouds")).toEqual(["Sabrina Carpenter", "Espresso", false]);
});

test("altered copies are dropped unless the search asked for them", () => {
  expect(mod.isAlteredCopy("Espresso (8D AUDIO)", "espresso")).toBe(true);
  expect(mod.isAlteredCopy("greedy slowed down", "greedy")).toBe(true);
  expect(mod.isAlteredCopy("Gods Plan (432hz)", "gods plan")).toBe(true);
  expect(mod.isAlteredCopy("Numb (Live)", "numb")).toBe(true);
  expect(mod.isAlteredCopy("Numb (Live)", "numb live")).toBe(false);
  expect(mod.isAlteredCopy("Espresso (Sped Up)", "espresso sped up")).toBe(false);
  // Only bracketed: these are song titles.
  expect(mod.isAlteredCopy("Live Forever", "oasis")).toBe(false);
  expect(mod.isAlteredCopy("Clean", "taylor swift")).toBe(false);
});

test("the Invidious fallback keeps one upload per song, the artist's audio first, and drops reupload noise", async () => {
  const realFetch = globalThis.fetch;
  const row = (videoId: string, title: string, author: string, lengthSeconds: number, authorVerified = false) =>
    ({ type: "video", videoId, title, author, lengthSeconds, authorVerified });
  globalThis.fetch = (async (input: string | URL | Request) => {
    if (String(input).startsWith("https://music.youtube.com")) return new Response("no", { status: 403 });
    return Response.json([
      row("latinhype01", "Sabrina Carpenter - Espresso", "LatinHype", 176),
      row("officialaud", "Sabrina Carpenter - Espresso (Official Audio)", "Sabrina Carpenter", 176, true),
      row("reversed001", "Espresso - Sabrina Carpenter (Audio)", "Music Audio", 176),
      row("eightdaudio", "Sabrina Carpenter - Espresso (8D AUDIO)", "8D Dream", 177, true),
      row("hqaudio0001", "Kendrick Lamar   Not Like Us HQ Audio", "HypeMusic", 274),
      row("vacation001", "Sabrina Carpenter - Espresso (On Vacation Version) [Official Audio]", "Sabrina Carpenter", 176, true),
      row("lyricsonly1", "Tate McRae - greedy (Lyrics)", "7clouds", 132, true),
      row("chappellomv", "Chappell Roan - Pink Pony Club (Official Music Video)", "Chappell Roan", 281, true),
      row("chappellaud", "Chappell Roan - Pink pony club (Official Audio)", " Too Much Music", 259),
    ]);
  }) as typeof fetch;
  process.env.YTM_INVIDIOUS_INSTANCE = "inv.test";
  try {
    const found = await mod.ytmSearch("espresso fallback", 10);
    expect(found.map((t) => [t.id, t.artist, t.title])).toEqual([
      ["officialaud", "Sabrina Carpenter", "Espresso"],
      ["vacation001", "Sabrina Carpenter", "Espresso (On Vacation Version)"],
      ["lyricsonly1", "Tate McRae", "greedy"],
      // The video has a 20 s intro; a reupload of the audio is the better copy.
      ["chappellaud", "Chappell Roan", "Pink pony club"],
    ]);
  } finally {
    globalThis.fetch = realFetch;
    delete process.env.YTM_INVIDIOUS_INSTANCE;
  }
});

test("hour-long innertube songs are parsed as such and dropped", async () => {
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async () =>
    Response.json({
      contents: [
        innertubeRow("vidLofiHour", "Lofi Rain (1 Hour)", "Zyra Music", "1:00:00"),
        innertubeRow("vidLongSong", "All Too Well (10 Minute Version)", "Taylor Swift", "10:13"),
      ],
    })) as unknown as typeof fetch;
  try {
    expect((await mod.ytmSearch("long songs", 5)).map((t) => [t.id, t.duration])).toEqual([["vidLongSong", 613]]);
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("lean innertube rows keep their i.ytimg.com art as is and lose the Topic suffix", async () => {
  const realFetch = globalThis.fetch;
  const withArt = (row: ReturnType<typeof innertubeRow>, url: string) => ({
    musicResponsiveListItemRenderer: {
      ...row.musicResponsiveListItemRenderer,
      thumbnail: { musicThumbnailRenderer: { thumbnail: { thumbnails: [{ url }] } } },
    },
  });
  globalThis.fetch = (async () =>
    Response.json({
      contents: [
        withArt(
          innertubeRow("vidLeanRow1", "Espresso", "Sabrina Carpenter - Topic", "2:56"),
          "https://i.ytimg.com/vi/vidLeanRow1/sddefault.jpg?sqp=-oaymwEWCJADEOEC",
        ),
        withArt(innertubeRow("vidFullRow1", "Juno", "Sabrina Carpenter", "3:43"), "https://lh3.googleusercontent.com/abc=w60-h60-l90-rj"),
      ],
    })) as unknown as typeof fetch;
  try {
    const [lean, full] = await mod.ytmSearch("lean layout", 5);
    expect(lean).toMatchObject({ artist: "Sabrina Carpenter", thumb: "https://i.ytimg.com/vi/vidLeanRow1/sddefault.jpg?sqp=-oaymwEWCJADEOEC" });
    expect(full.thumb).toBe("https://lh3.googleusercontent.com/abc=w226-h226-l90");
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("a catalogue cover at the right runtime loses to an upload that credits the artist", async () => {
  const realFetch = globalThis.fetch;
  const row = (videoId: string, title: string, author: string, lengthSeconds: number) =>
    ({ type: "video", videoId, title, author, lengthSeconds });
  let uploads: ReturnType<typeof row>[] = [];
  globalThis.fetch = (async (input: string | URL | Request) => {
    if (String(input).startsWith("https://music.youtube.com"))
      return Response.json({
        contents: [
          innertubeRow("vidPianoCov", 'Deep Docks (from "Silksong")', "Sheet Music Boss", "3:06"),
          innertubeRow("vidOtherSng", "Hornet", "Christopher Larkin & David Peacock", "3:04"),
        ],
      });
    return Response.json(uploads);
  }) as typeof fetch;
  process.env.YTM_INVIDIOUS_INSTANCE = "inv.test";
  try {
    uploads = [
      row("vidExtended", "Hollow Knight Silksong : Deep Docks OST EXTENDED", "Purehollow", 500),
      row("vidTutorial", "Deep Docks (from Silksong) - Piano Tutorial", "Sheet Music Boss", 190),
      row("vidReupload", "Deep Docks - Christopher Larkin (Hollow Knight: Silksong Original Soundtrack)", "Free Music", 186),
    ];
    expect(await mod.ytmResolveVideoId("Christopher Larkin", "Deep Docks", 186)).toBe("vidReupload");

    // The artist's Topic channel titles its uploads with the song alone.
    uploads = [row("vidTopicUpl", "Deep Docks", "Christopher Larkin - Topic", 186)];
    expect(await mod.ytmResolveVideoId("Christopher Larkin", "Deep Docks", 186)).toBe("vidTopicUpl");

    // No upload credits the artist either: a piano cover of the right length
    // is not the track, so nothing is returned rather than the cover.
    uploads = [];
    expect(await mod.ytmResolveVideoId("Christopher Larkin", "Deep Docks", 186)).toBeNull();
  } finally {
    globalThis.fetch = realFetch;
    delete process.env.YTM_INVIDIOUS_INSTANCE;
  }
});

test("the artist's own catalogue track wins over an earlier cover, bracket suffixes aside", async () => {
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async () =>
    Response.json({
      contents: [
        innertubeRow("vidCoverRow", "Dreams", "Tribute Band", "4:17"),
        innertubeRow("vidRemaster", "Dreams (2004 Remaster)", "Fleetwood Mac", "4:18"),
      ],
    })) as unknown as typeof fetch;
  try {
    expect(await mod.ytmResolveVideoId("Fleetwood Mac", "Dreams", 257)).toBe("vidRemaster");
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("a same-titled song by another artist never stands in for the one asked for", async () => {
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async () =>
    Response.json({
      contents: [
        innertubeRow("vidWrongArt", "Cellar", "Someone Else", "4:32"),
        innertubeRow("vidRightOne", "Cellar", "Johan Johnson, Peter Hont & Minecraft", "4:32"),
      ],
    })) as unknown as typeof fetch;
  try {
    expect(await mod.ytmResolveVideoId("Peter Hont", "Cellar", 272)).toBe("vidRightOne");
    expect(await mod.ytmResolveVideoId("Peter Hont", "Cellar", 400)).toBeNull();
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("a remix or live cut is not the studio track, and is the track when asked for", () => {
  expect(mod.sameVersion("Song - Remix", "Song")).toBe(false);
  expect(mod.sameVersion("Song (Live)", "Song")).toBe(false);
  expect(mod.sameVersion("Song", "Song (Live)")).toBe(false);
  expect(mod.sameVersion("Song (2011 Remaster)", "Song")).toBe(true);
  expect(mod.sameVersion("Live Forever", "Live Forever")).toBe(true);
  expect(mod.sameVersion("Song (Live)", "Song - Live at Wembley")).toBe(true);
});

test("a saved runtime that is off by a few seconds still finds the artist's own track, but not a different artist's", async () => {
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async () =>
    Response.json({
      contents: [
        innertubeRow("vidWrongArt2", "Cellar", "Someone Else", "4:43"),
        innertubeRow("vidRightTwo", "Cellar", "Johan Johnson, Peter Hont & Minecraft", "4:32"),
      ],
    })) as unknown as typeof fetch;
  try {
    // Qobuz said 283s, YouTube Music has 272s.
    expect(await mod.ytmResolveVideoId("Johan Johnson, Peter Hont, Minecraft", "Cellar", 283)).toBe("vidRightTwo");
    // A different cut by a minute is not the same track.
    expect(await mod.ytmResolveVideoId("Johan Johnson, Peter Hont, Minecraft", "Cellar", 400)).toBeNull();
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("with innertube refusing, an artist's Topic upload is found by quoting the title and one credited name", async () => {
  const realFetch = globalThis.fetch;
  const row = (videoId: string, title: string, author: string, lengthSeconds: number) =>
    ({ type: "video", videoId, title, author, lengthSeconds });
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = String(input);
    if (url.startsWith("https://music.youtube.com")) return new Response("no", { status: 403 });
    // The loose "artist title audio" search never surfaces the Topic upload.
    if (decodeURIComponent(url).includes('q="The Arch-Illager" "Peter Hont"'))
      return Response.json([row("vidTopicArch", "The Arch-Illager", "Peter Hont - Topic", 166)]);
    return Response.json([row("vidOtherTrk", "Ghast", "Peter Hont - Topic", 172)]);
  }) as typeof fetch;
  process.env.YTM_INVIDIOUS_INSTANCE = "inv.test";
  try {
    expect(await mod.ytmResolveVideoId("Peter Hont, Minecraft", "The Arch-Illager", 165)).toBe("vidTopicArch");
  } finally {
    globalThis.fetch = realFetch;
    delete process.env.YTM_INVIDIOUS_INSTANCE;
  }
});

test("without YTM_BASE the source is off: nothing is searched or downloaded", async () => {
  const realFetch = globalThis.fetch;
  const base = process.env.YTM_BASE;
  delete process.env.YTM_BASE;
  let calls = 0;
  globalThis.fetch = (async () => {
    calls++;
    return new Response("no", { status: 500 });
  }) as unknown as typeof fetch;
  try {
    expect(mod.ytmConfigured()).toBe(false);
    await expect(mod.ytmSearch("unconfigured base query", 5)).rejects.toBeInstanceOf(mod.YtmNotConfiguredError);
    await expect(mod.ytmAudioFile("noBaseSet01")).rejects.toBeInstanceOf(mod.YtmNotConfiguredError);
    expect(calls).toBe(0);
  } finally {
    globalThis.fetch = realFetch;
    process.env.YTM_BASE = base;
  }
});

test("search goes to whatever YTM_BASE names, with matching origin headers", async () => {
  const realFetch = globalThis.fetch;
  const base = process.env.YTM_BASE;
  process.env.YTM_BASE = "https://ytm.example.org/";
  const seen: { url: string; origin: string | null }[] = [];
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    seen.push({ url: String(input), origin: new Headers(init?.headers).get("origin") });
    return Response.json({});
  }) as unknown as typeof fetch;
  try {
    expect(mod.ytmBase()).toBe("https://ytm.example.org");
    expect(await mod.ytmSearch("custom base query", 5)).toEqual([]);
    expect(seen).toEqual([
      { url: "https://ytm.example.org/youtubei/v1/search?prettyPrint=false", origin: "https://ytm.example.org" },
    ]);
  } finally {
    globalThis.fetch = realFetch;
    process.env.YTM_BASE = base;
  }
});
