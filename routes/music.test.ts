import { afterAll, expect, test } from "bun:test";
import Fastify from "fastify";
import { browseRows, lookupTidalAlbum, lookupTidalArtist, musicRoutes, uniqueReleases } from "./music";

const realFetch = globalThis.fetch;
afterAll(() => {
  globalThis.fetch = realFetch;
});

const tidalTrack = (id: number, title: string, artist = "Band") => ({
  id,
  title,
  duration: 200,
  explicit: false,
  isrc: "X",
  artists: [{ id: 2, name: artist }],
  album: { id: 3, title: "Album", cover: "a-b" },
});

/** Answers the Tidal token request, then hands every other request's path
 * (with any TIDAL_PROXY prefix dropped) to `route`; undefined is a 404. */
function mockTidal(route: (path: string, url: URL) => unknown): string[] {
  const seen: string[] = [];
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = new URL(String(input));
    if (url.hostname === "auth.tidal.com") return Response.json({ access_token: "t", expires_in: 3600 });
    const path = url.pathname.replace(/^.*?(?=\/v1\/)/, "");
    seen.push(`${path}${url.search}`);
    const body = await route(path, url);
    return body === undefined ? new Response("nope", { status: 404 }) : Response.json(body);
  }) as typeof fetch;
  return seen;
}

async function app() {
  const server = Fastify();
  await server.register(musicRoutes);
  return server;
}

test("browse rows come from Tidal playlists; a failed row is empty and the set is asked for again", async () => {
  let failRock = true;
  let playlistCalls = 0;
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = String(input);
    if (url.startsWith("https://auth.tidal.com")) return Response.json({ access_token: "t", expires_in: 3600 });
    if (url.includes("/featured/new/albums"))
      return Response.json({ items: [{ id: 9, title: "New LP", artists: [{ id: 2, name: "Band" }], cover: "c-d" }] });
    playlistCalls++;
    if (failRock && url.includes("/playlists/94fe2b9b")) return new Response("down", { status: 503 });
    return Response.json({
      items: [{ id: 1, title: "Song", duration: 200, explicit: false, isrc: "X", artists: [{ id: 2, name: "Band" }], album: { id: 3, title: "Album", cover: "a-b" } }],
    });
  }) as typeof fetch;
  try {
    const first = await browseRows();
    expect(first.rows.rock).toEqual([]);
    expect(first.rows.hits[0]).toMatchObject({ id: 1, title: "Song", artist: "Band", albumId: 3, source: "tidal" });
    expect(first.albums).toEqual([expect.objectContaining({ id: 9, title: "New LP", artist: { id: 2, name: "Band" } })]);
    failRock = false;
    expect((await browseRows()).rows.rock).toHaveLength(1);
    const calls = playlistCalls;
    await browseRows();
    expect(playlistCalls).toBe(calls);
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("an album page lists every track across pages and leaves out music videos", async () => {
  mockTidal((path, url) => {
    if (path === "/v1/albums/77")
      return { id: 77, title: "LP", artist: { id: 2, name: "Band" }, numberOfTracks: 150, type: "ALBUM", cover: "a-b" };
    if (path === "/v1/albums/77/items") {
      const offset = Number(url.searchParams.get("offset") ?? 0);
      const n = offset === 0 ? 100 : 50;
      const items = Array.from({ length: n }, (_, i) => ({ type: "track", item: tidalTrack(offset + i + 1, `T${offset + i + 1}`) }));
      if (offset === 0) items[1] = { type: "video", item: tidalTrack(9999, "Video") };
      return { items, totalNumberOfItems: 150 };
    }
  });
  const server = await app();
  const res = await server.inject({ method: "GET", url: "/api/music/album/77" });
  expect(res.statusCode).toBe(200);
  const { album, tracks } = res.json();
  expect(album).toMatchObject({ id: 77, title: "LP", artist: { id: 2, name: "Band" }, type: "ALBUM" });
  expect(tracks).toHaveLength(149);
  expect(tracks.some((t: { title: string }) => t.title === "Video")).toBe(false);
  expect(tracks.at(-1)).toMatchObject({ id: 150, albumId: 3, source: "tidal" });
});

test("album and artist ids must be numeric, and nothing is fetched for a bad one", async () => {
  const seen = mockTidal(() => ({}));
  const server = await app();
  for (const url of ["/api/music/album/abc", "/api/music/album/1%2F..%2Fx", "/api/music/artist/12a", "/api/music/album/1x/similar"])
    expect((await server.inject({ method: "GET", url })).statusCode).toBe(400);
  expect(seen).toEqual([]);
});

test("an artist page has albums and EPs newest first, one per edition, and top tracks", async () => {
  mockTidal((path, url) => {
    if (path === "/v1/artists/2") return { id: 2, name: "Band", picture: "p-q" };
    if (path === "/v1/artists/2/albums" && url.searchParams.get("filter") === "EPSANDSINGLES")
      return { items: [{ id: 20, title: "Single", type: "SINGLE", releaseDate: "2025-01-01", artist: { id: 2, name: "Band" } }] };
    if (path === "/v1/artists/2/albums")
      return {
        items: [
          { id: 10, title: "Old LP", releaseDate: "2019-01-01", artist: { id: 2, name: "Band" } },
          { id: 11, title: "New LP", releaseDate: "2024-01-01", artist: { id: 2, name: "Band" } },
          { id: 12, title: "New LP", releaseDate: "2024-01-01", artist: { id: 2, name: "Band" } },
        ],
      };
    if (path === "/v1/artists/2/toptracks") return { items: [tidalTrack(1, "Hit")] };
  });
  const res = await (await app()).inject({ method: "GET", url: "/api/music/artist/2" });
  expect(res.statusCode).toBe(200);
  const page = res.json();
  expect(page.artist).toMatchObject({ id: 2, name: "Band" });
  expect(page.albums.map((a: { id: number }) => a.id)).toEqual([11, 10]);
  expect(page.eps.map((a: { id: number }) => a.id)).toEqual([20]);
  expect(page.tracks[0]).toMatchObject({ id: 1, title: "Hit" });
});

test("a single with no similar albums shows the artist's other releases", async () => {
  mockTidal((path) => {
    if (path === "/v1/albums/5/similar") return undefined;
    if (path === "/v1/albums/5") return { id: 5, title: "Espresso", artist: { id: 2, name: "Band" } };
    if (path === "/v1/artists/2/albums") return { items: [{ id: 6, title: "LP", artist: { id: 2, name: "Band" } }] };
    if (path.startsWith("/v1/artists/2/albums")) return { items: [] };
  });
  const res = await (await app()).inject({ method: "GET", url: "/api/music/album/5/similar" });
  expect(res.json().albums.map((a: { id: number }) => a.id)).toEqual([6]);
});

test("when api.tidal.com fails the album page comes from a hifi mirror", async () => {
  mockTidal((path) => {
    if (path === "/album/") return { data: { id: 78, title: "Mirror LP", numberOfTracks: 1, items: [{ item: tidalTrack(1, "Song") }] } };
    if (path.startsWith("/v1/")) return undefined;
  });
  const res = await (await app()).inject({ method: "GET", url: "/api/music/album/78" });
  expect(res.statusCode).toBe(200);
  expect(res.json()).toMatchObject({ album: { id: 78, title: "Mirror LP" }, tracks: [{ id: 1, title: "Song" }] });
});

test("search results carry Tidal albums and artists whatever the track source", async () => {
  mockTidal((path) => {
    if (path === "/v1/search/tracks") return { items: [tidalTrack(1, "Song")] };
    if (path === "/v1/search")
      return {
        albums: { items: [{ id: 30, title: "LP", artists: [{ id: 2, name: "Band" }] }] },
        artists: { items: [{ id: 2, name: "Band" }] },
      };
  });
  const res = await (await app()).inject({ method: "GET", url: "/api/music/search?q=band&limit=5&source=tidal" });
  expect(res.json()).toMatchObject({ items: [{ id: 1 }], albums: [{ id: 30, title: "LP" }], artists: [{ id: 2, name: "Band" }] });
});

test("an album is found by name: exact title and artist, then an edition, never another artist's", async () => {
  mockTidal((path, url) => {
    if (path !== "/v1/search/albums") return undefined;
    const q = url.searchParams.get("query") ?? "";
    if (q.includes("NewJeans"))
      return { items: [{ id: 5, title: "뉴진스", artists: [{ name: "NewJeans" }] }, { id: 6, title: "다른", artists: [{ name: "NewJeans" }] }] };
    return {
      items: [
        { id: 1, title: "Greatest Hits", artist: { name: "Other" } },
        { id: 2, title: "Greatest Hits", artists: [{ name: "Queen" }] },
        { id: 3, title: "A Night at the Opera (Deluxe Edition)", artists: [{ name: "Queen" }] },
      ],
    };
  });
  expect(await lookupTidalAlbum("Queen", "Greatest Hits")).toBe(2);
  expect(await lookupTidalAlbum("Queen, David Bowie", "Greatest Hits")).toBe(2);
  expect(await lookupTidalAlbum("Queen", "A Night at the Opera")).toBe(3);
  expect(await lookupTidalAlbum("Nobody", "Greatest Hits")).toBeNull();
  expect(await lookupTidalAlbum("", "Greatest Hits")).toBe(1);
  // "Unknown" is a missing artist, not an artist called Unknown.
  expect(await lookupTidalAlbum("Unknown", "Greatest Hits")).toBe(1);
  // Hangul normalises to nothing; that must not match the first album.
  expect(await lookupTidalAlbum("NewJeans", "다른")).toBe(6);
});

test("album editions collapse to one, the explicit stereo one, in the first edition's place", () => {
  const a = (id: number, title: string, extra: object = {}) => ({
    id,
    title,
    releaseDate: "2022-07-15",
    artist: { id: 1, name: "Band" },
    ...extra,
  });
  const out = uniqueReleases([
    a(1, "emails i can’t send", { audioModes: ["DOLBY_ATMOS"] }),
    a(2, "Other LP"),
    a(3, "emails i can't send", { explicit: true, audioModes: ["STEREO"] }),
    a(4, "emails i can't send", { audioModes: ["STEREO"] }),
    // Same title, different release: a second "Greatest Hits" is its own album.
    a(5, "Greatest Hits", { releaseDate: "1981-10-26" }),
    a(6, "Greatest Hits", { releaseDate: "1991-10-28" }),
    a(7, "Other LP", { artist: { id: 9, name: "Other Band" } }),
  ]);
  expect(out.map((x) => x.id)).toEqual([3, 2, 5, 6, 7]);
});

test("an album page is asked of Tidal once and then cached, but a failure is asked again", async () => {
  let up = false;
  const seen = mockTidal((path) => {
    if (!up) return undefined;
    if (path === "/v1/albums/88") return { id: 88, title: "Cached", artist: { id: 2, name: "Band" } };
    if (path === "/v1/albums/88/items") return { items: [{ type: "track", item: tidalTrack(1, "A") }], totalNumberOfItems: 1 };
  });
  const server = await app();
  expect((await server.inject({ method: "GET", url: "/api/music/album/88" })).statusCode).not.toBe(200);
  up = true;
  for (let i = 0; i < 3; i++) expect((await server.inject({ method: "GET", url: "/api/music/album/88" })).statusCode).toBe(200);
  // Two failed calls, then the two that answered; nothing after that.
  expect(seen.filter((p) => p.startsWith("/v1/albums/88"))).toHaveLength(4);
});

test("artist names are tried whole before they are split", async () => {
  mockTidal((path, url) => {
    if (path !== "/v1/search/artists") return undefined;
    const q = url.searchParams.get("query");
    const all = [
      { id: 1, name: "Tyler, The Creator" },
      { id: 2, name: "Tyler" },
      { id: 3, name: "Lil Nas X" },
      { id: 4, name: "X Ambassadors" },
      { id: 5, name: "Drake" },
    ];
    return { items: all.filter((a) => a.name.toLowerCase().includes((q ?? "").toLowerCase().split(",")[0])) };
  });
  expect(await lookupTidalArtist("Tyler, The Creator")).toBe(1);
  expect(await lookupTidalArtist("Lil Nas X")).toBe(3);
  expect(await lookupTidalArtist("X Ambassadors")).toBe(4);
  expect(await lookupTidalArtist("Drake, 21 Savage")).toBe(5);
  expect(await lookupTidalArtist("Unknown")).toBeNull();
});

test("a Tidal error on similar albums is not cached as the artist fallback", async () => {
  let similarStatus = 503;
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = new URL(String(input));
    if (url.hostname === "auth.tidal.com") return Response.json({ access_token: "t", expires_in: 3600 });
    const path = url.pathname.replace(/^.*?(?=\/v1\/)/, "");
    if (path === "/v1/albums/41/similar") return new Response("x", { status: similarStatus });
    if (path === "/v1/albums/41") return Response.json({ id: 41, title: "Single", artist: { id: 2, name: "Band" } });
    if (path === "/v1/artists/2/albums") return Response.json({ items: [{ id: 42, title: "LP", artist: { id: 2, name: "Band" } }] });
    return new Response("nope", { status: 404 });
  }) as typeof fetch;
  const server = await app();
  expect((await server.inject({ method: "GET", url: "/api/music/album/41/similar" })).json().albums).toEqual([]);
  similarStatus = 404;
  expect((await server.inject({ method: "GET", url: "/api/music/album/41/similar" })).json().albums.map((a: { id: number }) => a.id)).toEqual([42]);
});

test("concurrent requests for one album share one Tidal request", async () => {
  const seen = mockTidal((path) => {
    if (path === "/v1/albums/55") return { id: 55, title: "Shared", artist: { id: 2, name: "Band" } };
    if (path === "/v1/albums/55/items") return { items: [{ type: "track", item: tidalTrack(1, "A") }], totalNumberOfItems: 1 };
  });
  const server = await app();
  const res = await Promise.all([1, 2, 3].map(() => server.inject({ method: "GET", url: "/api/music/album/55" })));
  expect(res.map((r) => r.statusCode)).toEqual([200, 200, 200]);
  expect(seen.filter((p) => p.startsWith("/v1/albums/55"))).toHaveLength(2);
});

test("a search is not held up by a Tidal catalog that doesn't answer", async () => {
  mockTidal((path) => {
    if (path === "/v1/search/tracks") return { items: [tidalTrack(1, "Song")] };
    if (path === "/v1/search") return new Promise(() => {});
  });
  const t = Date.now();
  const res = await (await app()).inject({ method: "GET", url: "/api/music/search?q=slow+catalog&limit=5&source=tidal" });
  expect(Date.now() - t).toBeLessThan(3500);
  expect(res.json()).toMatchObject({ items: [{ id: 1 }], albums: [], artists: [] });
});
