import { afterAll, beforeAll, expect, test } from "bun:test";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";

let dir: string;
let mod: typeof import("./artCache");

beforeAll(async () => {
  dir = await fsp.mkdtemp(path.join(os.tmpdir(), "art-"));
  process.env.ART_CACHE_DIR = dir;
  mod = await import("./artCache");
});
afterAll(async () => {
  await fsp.rm(dir, { recursive: true, force: true });
});

test("a legacy flat entry is served and moved into its shard", async () => {
  const url = "https://resources.tidal.com/images/legacy.jpg";
  // Mirror cacheKey(): sha256 of the url, first 32 hex chars.
  const key = new Bun.CryptoHasher("sha256").update(url).digest("hex").slice(0, 32);
  await fsp.writeFile(path.join(dir, key), Buffer.from("jpegbytes"));
  await fsp.writeFile(path.join(dir, `${key}.type`), "image/jpeg");

  const art = await mod.getArt(url);
  expect(art.contentType).toBe("image/jpeg");
  expect(art.size).toBe(9);
  expect(art.file).toBe(path.join(dir, key.slice(0, 2), key));
  await expect(fsp.stat(path.join(dir, key))).rejects.toThrow();
  const again = await mod.getArt(url);
  expect(again.file).toBe(art.file);
});

test("a failed fetch is remembered and replayed without going upstream", async () => {
  const url = "https://resources.tidal.com/images/does-not-exist.jpg";
  const realFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = (async () => {
    calls++;
    return new Response("nope", { status: 429 });
  }) as unknown as typeof fetch;
  try {
    await expect(mod.getArt(url)).rejects.toThrow("upstream 429");
    await expect(mod.getArt(url)).rejects.toThrow(mod.ArtUnavailableError);
    expect(calls).toBe(1);
    expect(mod.artCacheStats().negative).toBeGreaterThanOrEqual(1);
  } finally {
    globalThis.fetch = realFetch;
  }
});

function artKey(url: string) {
  return new Bun.CryptoHasher("sha256").update(url).digest("hex").slice(0, 32);
}

test("a cover kept in the store comes back without going upstream", async () => {
  const url = "https://resources.tidal.com/images/stored.jpg";
  const { get } = mod.artStore;
  const realFetch = globalThis.fetch;
  let upstream = 0;
  globalThis.fetch = (async () => {
    upstream++;
    return new Response("nope", { status: 500 });
  }) as unknown as typeof fetch;
  mod.artStore.get = async (k) =>
    k === `art/${artKey(url)}` ? new Response("storedbytes", { headers: { "content-type": "image/png" } }) : null;
  try {
    const art = await mod.getArt(url);
    expect(art.contentType).toBe("image/png");
    expect(await fsp.readFile(art.file, "utf8")).toBe("storedbytes");
    expect(upstream).toBe(0);
  } finally {
    globalThis.fetch = realFetch;
    mod.artStore.get = get;
  }
});

test("a cover fetched upstream is also put in the store", async () => {
  const url = "https://resources.tidal.com/images/fresh.jpg";
  const { put } = mod.artStore;
  const realFetch = globalThis.fetch;
  const puts: string[] = [];
  globalThis.fetch = (async () =>
    new Response("jpg", { headers: { "content-type": "image/jpeg" } })) as unknown as typeof fetch;
  mod.artStore.put = async (k) => {
    puts.push(k);
  };
  try {
    await mod.getArt(url);
    expect(puts).toEqual([`art/${artKey(url)}`]);
  } finally {
    globalThis.fetch = realFetch;
    mod.artStore.put = put;
  }
});
