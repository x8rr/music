import { describe, expect, test } from "bun:test";
import { OPEN_AFTER_FAILURES, OPEN_MS, PROBE_STALE_MS, SourceBreaker, SourceParkedError, UNREACHABLE_OPEN_MS } from "./sourceBreaker";

function make() {
  let now = 1_000_000;
  const lines: string[] = [];
  const b = new SourceBreaker((l) => lines.push(l), () => now);
  return { b, lines, advance: (ms: number) => (now += ms) };
}

describe("SourceBreaker", () => {
  test("parks after consecutive failures, rejects without a loader while parked", () => {
    const { b, lines } = make();
    for (let i = 0; i < OPEN_AFTER_FAILURES; i++) b.recordFailure("qobuz", new Error("HTTP 500"));
    expect(b.isOpen("qobuz")).toBe(true);
    expect(() => b.assertAvailable("qobuz")).toThrow(SourceParkedError);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("parked for 60s");
  });

  test("an unreachable host parks longer", () => {
    const { b, advance } = make();
    for (let i = 0; i < OPEN_AFTER_FAILURES; i++) b.recordFailure("scdlp", new Error("Unable to connect."));
    advance(OPEN_MS + 1);
    expect(b.isOpen("scdlp")).toBe(true);
    advance(UNREACHABLE_OPEN_MS);
    expect(b.isOpen("scdlp")).toBe(false); // the probe
  });

  test("one probe after expiry; success closes with one log line, failure re-parks silently", () => {
    const { b, lines, advance } = make();
    for (let i = 0; i < OPEN_AFTER_FAILURES; i++) b.recordFailure("ytm", new Error("HTTP 429"));
    advance(OPEN_MS + 1);
    expect(b.isOpen("ytm")).toBe(false); // probe let through
    expect(b.isOpen("ytm")).toBe(true); // second caller waits for the probe
    b.recordFailure("ytm", new Error("HTTP 429"));
    expect(b.isOpen("ytm")).toBe(true);
    expect(lines).toHaveLength(1);
    advance(OPEN_MS + 1);
    expect(b.isOpen("ytm")).toBe(false);
    b.recordSuccess("ytm");
    expect(b.isOpen("ytm")).toBe(false);
    expect(lines).toHaveLength(2);
    expect(lines[1]).toContain("unparked");
  });

  test("a probe that never reports back gives up its slot", () => {
    const { b, advance } = make();
    for (let i = 0; i < OPEN_AFTER_FAILURES; i++) b.recordFailure("qobuz", new Error("HTTP 500"));
    advance(OPEN_MS + 1);
    expect(b.isOpen("qobuz")).toBe(false); // probe let through, never settles
    advance(PROBE_STALE_MS - 1);
    expect(b.isOpen("qobuz")).toBe(true);
    advance(1);
    expect(b.isOpen("qobuz")).toBe(false); // a fresh probe
    expect(b.isOpen("qobuz")).toBe(true);
    b.recordSuccess("qobuz");
    expect(b.isOpen("qobuz")).toBe(false);
  });

  test("failure logging is sampled within a streak", () => {
    const { b } = make();
    const logged: boolean[] = [];
    for (let i = 0; i < 4; i++) logged.push(b.recordFailure("soundcloud", new Error("x")));
    expect(logged).toEqual([true, true, true, false]);
  });
});
