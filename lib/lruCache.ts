export const DEFAULT_TTL_MS = 60_000;
export const DEFAULT_MAX = 10_000;

type Entry<T> = { at: number; value: T };

/** A size- and age-bounded map: the least recently read entry goes first. */
export class LruCache<T> {
  private map = new Map<string, Entry<T>>();
  hits = 0;
  misses = 0;

  constructor(
    public readonly name: string,
    private readonly max = DEFAULT_MAX,
    private readonly ttlMs = DEFAULT_TTL_MS,
    private readonly now: () => number = Date.now,
  ) {}

  get(key: string): T | undefined {
    const e = this.map.get(key);
    if (!e) {
      this.misses++;
      return undefined;
    }
    if (this.now() - e.at > this.ttlMs) {
      this.map.delete(key);
      this.misses++;
      return undefined;
    }
    // Map iteration is insertion order, so re-inserting is the LRU touch.
    this.map.delete(key);
    this.map.set(key, e);
    this.hits++;
    return e.value;
  }

  set(key: string, value: T): void {
    this.map.delete(key);
    this.map.set(key, { at: this.now(), value });
    if (this.map.size > this.max) {
      const oldest = this.map.keys().next().value;
      if (oldest !== undefined) this.map.delete(oldest);
    }
  }

  /** Returns the cached value or loads, stores and returns a fresh one. */
  async getOrLoad(key: string, load: () => Promise<T>): Promise<T> {
    const hit = this.get(key);
    if (hit !== undefined) return hit;
    const value = await load();
    this.set(key, value);
    return value;
  }

  delete(key: string): void {
    this.map.delete(key);
  }

  clear(): void {
    this.map.clear();
  }

  get size(): number {
    return this.map.size;
  }
}
