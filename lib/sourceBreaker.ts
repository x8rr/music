/**
 * Per-upstream circuit breaker for the music sources.
 *
 * When a source is down (Tidal unreachable, the SoundCloud service stopped,
 * YouTube refusing downloads) every search and every stream still tried it,
 * waited for the timeout, and wrote a line to the error log: forty thousand
 * lines an hour, and a connect timeout per request for every listener. After
 * a few consecutive failures the source is parked; while parked, callers get
 * a synchronous rejection with no log line and no network, and a single probe
 * is let through once the park expires.
 */

export class SourceParkedError extends Error {
  constructor(source: string) {
    super(`${source} is parked after repeated failures`);
    this.name = "SourceParkedError";
  }
}

export const OPEN_AFTER_FAILURES = 5;
export const OPEN_MS = 60_000;
/** A host that cannot be reached at all is not coming back in a minute. */
export const UNREACHABLE_OPEN_MS = 5 * 60_000;
/** A probe that has not reported back by now is taken for lost. A request
 * that never settles would otherwise hold the probe slot until the process
 * restarts, with the source reading as parked the whole time. */
export const PROBE_STALE_MS = 60_000;

const UNREACHABLE_RE = /unable to connect|unreachable|ECONNREFUSED|ENOTFOUND|EHOSTUNREACH|timed out|aborted/i;

type State = {
  failures: number;
  openUntil: number;
  probing: boolean;
  probeAt: number;
  opens: number;
  totalFailures: number;
};

export type SourceBreakerLog = (line: string) => void;

export class SourceBreaker {
  private readonly states = new Map<string, State>();

  constructor(
    private readonly log: SourceBreakerLog = (line) => console.warn(line),
    private readonly now: () => number = Date.now,
  ) {}

  private state(source: string): State {
    let s = this.states.get(source);
    if (!s) {
      s = { failures: 0, openUntil: 0, probing: false, probeAt: 0, opens: 0, totalFailures: 0 };
      this.states.set(source, s);
    }
    return s;
  }

  /** True while the source should not be tried. Lets one probe through per expiry. */
  isOpen(source: string): boolean {
    const s = this.state(source);
    if (s.openUntil === 0) return false;
    if (this.now() < s.openUntil) return true;
    if (s.probing && this.now() - s.probeAt < PROBE_STALE_MS) return true;
    s.probing = true;
    s.probeAt = this.now();
    return false;
  }

  /** Throws SourceParkedError when the source is parked; otherwise a no-op. */
  assertAvailable(source: string): void {
    if (this.isOpen(source)) throw new SourceParkedError(source);
  }

  recordSuccess(source: string): void {
    const s = this.state(source);
    if (s.openUntil !== 0) this.log(`[source] ${source} is back; unparked`);
    s.failures = 0;
    s.openUntil = 0;
    s.probing = false;
  }

  /**
   * Records a failure. Returns whether the caller should log it: the first
   * few in a streak and then one in fifty, so a dead source is visible without
   * filling the log with the same line.
   */
  recordFailure(source: string, err: unknown): boolean {
    const s = this.state(source);
    s.failures++;
    s.totalFailures++;
    const message = err instanceof Error ? err.message : String(err);
    const wasProbe = s.probing;
    s.probing = false;
    if (wasProbe || s.failures >= OPEN_AFTER_FAILURES) {
      const ms = UNREACHABLE_RE.test(message) ? UNREACHABLE_OPEN_MS : OPEN_MS;
      const reopened = s.openUntil !== 0;
      s.openUntil = this.now() + ms;
      if (!reopened) {
        s.opens++;
        this.log(
          `[source] ${source} parked for ${Math.round(ms / 1000)}s after ${s.failures} failures: ${message}`,
        );
      }
      return false;
    }
    return s.failures <= 3 || s.failures % 50 === 0;
  }

  stats(): Record<string, { parkedForSec: number; failures: number; opens: number; totalFailures: number }> {
    const out: Record<string, { parkedForSec: number; failures: number; opens: number; totalFailures: number }> = {};
    const now = this.now();
    for (const [source, s] of this.states) {
      out[source] = {
        parkedForSec: Math.max(0, Math.ceil((s.openUntil - now) / 1000)),
        failures: s.failures,
        opens: s.opens,
        totalFailures: s.totalFailures,
      };
    }
    return out;
  }
}

export const musicSourceBreaker = new SourceBreaker();
