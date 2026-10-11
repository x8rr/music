import { afterEach, beforeEach, expect, test } from "bun:test";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";
import { sweepCacheDir, touchIfStale } from "./cacheSweep";

let dir: string;
beforeEach(async () => {
  dir = await fsp.mkdtemp(path.join(os.tmpdir(), "sweep-"));
});
afterEach(async () => {
  await fsp.rm(dir, { recursive: true, force: true });
});

async function file(rel: string, bytes: number, ageMs: number) {
  const full = path.join(dir, rel);
  await fsp.mkdir(path.dirname(full), { recursive: true });
  await fsp.writeFile(full, Buffer.alloc(bytes, 1));
  const t = new Date(Date.now() - ageMs);
  await fsp.utimes(full, t, t);
  return full;
}

test("a directory that does not exist is a no-op", async () => {
  const r = await sweepCacheDir(path.join(dir, "missing"), { maxBytes: 1, maxAgeMs: 1 });
  expect(r).toEqual({ deleted: 0, freedBytes: 0, keptBytes: 0, passes: 0 });
});

test("removes files older than the age limit, keeps the rest, walks shards", async () => {
  const old = await file("ab/old", 100, 10 * 86_400_000);
  const fresh = await file("cd/fresh", 100, 1000);
  const tmp = await file("ef/x.tmp", 100, 10 * 86_400_000);
  const r = await sweepCacheDir(dir, { maxBytes: 10_000, maxAgeMs: 86_400_000 });
  expect(r.deleted).toBe(1);
  expect(r.freedBytes).toBe(100);
  await expect(fsp.stat(old)).rejects.toThrow();
  await expect(fsp.stat(fresh)).resolves.toBeDefined();
  await expect(fsp.stat(tmp)).resolves.toBeDefined();
});

test("halves the age threshold until the size cap is met", async () => {
  await file("a", 1000, 3 * 3_600_000);
  await file("b", 1000, 2 * 3_600_000);
  const keep = await file("c", 1000, 60_000);
  const r = await sweepCacheDir(dir, { maxBytes: 1500, maxAgeMs: 8 * 3_600_000 });
  expect(r.passes).toBeGreaterThan(1);
  expect(r.keptBytes).toBeLessThanOrEqual(1500);
  await expect(fsp.stat(keep)).resolves.toBeDefined();
});

test("touchIfStale bumps a stale atime and leaves a fresh one", async () => {
  const stale = await file("stale", 10, 2 * 3_600_000);
  const fresh = await file("fresh", 10, 60_000);
  const before = await fsp.stat(fresh);
  touchIfStale(stale, await fsp.stat(stale));
  touchIfStale(fresh, before);
  await Bun.sleep(50);
  const s = await fsp.stat(stale);
  expect(Date.now() - s.atimeMs).toBeLessThan(60_000);
  expect(s.mtimeMs).toBeLessThan(Date.now() - 3_600_000);
  expect((await fsp.stat(fresh)).atimeMs).toBe(before.atimeMs);
});
