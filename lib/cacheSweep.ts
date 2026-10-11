/**
 * Size- and age-bounded eviction for the on-disk caches (album art, ytm
 * audio). Neither had any eviction: the art cache reached a million files
 * and 34 GB, the audio cache 11 GB, on a box that also holds the database.
 *
 * Delegated to `find` and `du` in a child process on purpose. The first
 * version walked the tree with fs.opendir, and under Bun that materializes
 * every entry up front: a million Dirent objects (~120 MB of heap) pinned for
 * the whole sweep, plus a stat per file on the main thread, which showed up
 * as event-loop stalls of several hundred milliseconds. `find -delete`
 * streams the directory in its own process and costs this one nothing.
 *
 * Pass one removes anything not read for `maxAgeMs` (by atime, which still
 * advances daily under relatime), then measures what is left. If that is
 * still over `maxBytes`, the age is halved and the pass repeats, a few times
 * at most. Coarse, but it converges.
 */
import { promises as fsp } from "node:fs";

export type SweepOptions = {
  maxBytes: number;
  maxAgeMs: number;
};

export type SweepResult = { deleted: number; freedBytes: number; keptBytes: number; passes: number };

async function run(cmd: string[]): Promise<string> {
  const proc = Bun.spawn(cmd, { stdout: "pipe", stderr: "ignore" });
  const out = await new Response(proc.stdout).text();
  await proc.exited;
  return out;
}

async function dirExists(dir: string): Promise<boolean> {
  try {
    const st = await import("node:fs/promises").then((fsp) => fsp.stat(dir));
    return st.isDirectory();
  } catch {
    return false;
  }
}

/** Deletes files unread for more than `minutes`; returns count and bytes freed. */
async function deleteOlderThan(dir: string, minutes: number): Promise<{ deleted: number; freedBytes: number }> {
  // A write in progress is never touched; the writer renames it into place.
  // %s prints the size before -delete removes the file.
  const out = await run([
    "find", dir, "-type", "f", "-not", "-name", "*.tmp*", "-amin", `+${minutes}`, "-printf", "%s\n", "-delete",
  ]);
  let deleted = 0;
  let freedBytes = 0;
  for (const line of out.split("\n")) {
    if (!line) continue;
    deleted++;
    freedBytes += Number(line) || 0;
  }
  return { deleted, freedBytes };
}

async function totalBytes(dir: string): Promise<number> {
  const out = await run(["du", "-sb", dir]);
  return Number(out.split(/\s+/)[0]) || 0;
}

export async function sweepCacheDir(dir: string, opts: SweepOptions): Promise<SweepResult> {
  const result: SweepResult = { deleted: 0, freedBytes: 0, keptBytes: 0, passes: 0 };
  if (!(await dirExists(dir))) return result;
  let maxAge = opts.maxAgeMs;
  const HOUR = 60 * 60 * 1000;
  // Twelve, not six: six halvings of 60 days stop at ~1.9 days, and the art
  // cache's two-day working set sat at 44 GB against a 20 GB cap.
  for (let pass = 0; pass < 12; pass++) {
    result.passes++;
    const minutes = Math.max(1, Math.floor(maxAge / 60_000));
    const { deleted, freedBytes } = await deleteOlderThan(dir, minutes);
    result.deleted += deleted;
    result.freedBytes += freedBytes;
    result.keptBytes = await totalBytes(dir);
    if (result.keptBytes <= opts.maxBytes || maxAge <= HOUR) break;
    maxAge = Math.floor(maxAge / 2);
  }
  return result;
}

const SWEEP_INTERVAL_MS = 6 * 60 * 60 * 1000;
const TOUCH_AFTER_MS = 60 * 60 * 1000;

/**
 * Marks a cache hit as read. relatime moves atime at most once a day, so a
 * cover read a thousand times this morning looked no newer than one read
 * once, and an age-based sweep could not keep the popular ones. At most one
 * write per file per hour.
 */
export function touchIfStale(file: string, st: { atimeMs: number; mtime: Date }): void {
  if (Date.now() - st.atimeMs < TOUCH_AFTER_MS) return;
  fsp.utimes(file, new Date(), st.mtime).catch(() => {});
}

/** Sweeps `dir` on an interval, one sweep at a time, starting a minute after boot. */
export function startCacheSweep(label: string, dir: string, opts: SweepOptions): void {
  // Per sweep: one shared flag let the art sweep, started in the same tick,
  // turn the ytm sweep away every time, so the audio cache was never swept.
  let running = false;
  const go = async () => {
    if (running) return;
    running = true;
    const started = Date.now();
    try {
      const r = await sweepCacheDir(dir, opts);
      if (r.deleted > 0)
        console.log(
          `[cache-sweep] ${label}: removed ${r.deleted} files (${Math.round(r.freedBytes / 1048576)} MB), ` +
            `${Math.round(r.keptBytes / 1048576)} MB kept, ${r.passes} pass(es), ${Math.round((Date.now() - started) / 1000)}s`,
        );
    } catch (err) {
      console.error(`[cache-sweep] ${label} failed`, err);
    } finally {
      running = false;
    }
  };
  const first = setTimeout(() => void go(), 60_000);
  first.unref?.();
  const timer = setInterval(() => void go(), SWEEP_INTERVAL_MS);
  timer.unref?.();
}
