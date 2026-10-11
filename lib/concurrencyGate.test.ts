import { expect, test } from "bun:test";
import { ConcurrencyGate, GateBusyError } from "./concurrencyGate";

test("runs at most `max` jobs at once and hands slots to waiters", async () => {
  const gate = new ConcurrencyGate(2, 10);
  let active = 0;
  let peak = 0;
  const job = () =>
    gate.run(async () => {
      active++;
      peak = Math.max(peak, active);
      await Bun.sleep(10);
      active--;
      return "ok";
    });
  const results = await Promise.all(Array.from({ length: 6 }, job));
  expect(results).toEqual(Array(6).fill("ok"));
  expect(peak).toBe(2);
  expect(gate.stats()).toEqual({ active: 0, waiting: 0 });
});

test("refuses callers beyond the queue bound instead of queueing them", async () => {
  const gate = new ConcurrencyGate(1, 1);
  const release = Promise.withResolvers<void>();
  const first = gate.run(() => release.promise);
  const second = gate.run(async () => "second");
  await Bun.sleep(1);
  expect(gate.stats()).toEqual({ active: 1, waiting: 1 });
  await expect(gate.run(async () => "third")).rejects.toBeInstanceOf(GateBusyError);
  release.resolve();
  await first;
  await expect(second).resolves.toBe("second");
  expect(gate.stats()).toEqual({ active: 0, waiting: 0 });
});

test("a failing job releases its slot", async () => {
  const gate = new ConcurrencyGate(1, 1);
  await expect(gate.run(async () => { throw new Error("boom"); })).rejects.toThrow("boom");
  await expect(gate.run(async () => 1)).resolves.toBe(1);
  expect(gate.stats()).toEqual({ active: 0, waiting: 0 });
});
