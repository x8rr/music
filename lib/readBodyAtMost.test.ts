import { expect, test } from "bun:test";
import { readBodyAtMost } from "./readBodyAtMost";

function stream(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    pull(controller) {
      const next = chunks.shift();
      if (next) controller.enqueue(next);
      else controller.close();
    },
  });
}

test("collects a body under the cap", async () => {
  const buf = await readBodyAtMost(stream([new Uint8Array([1, 2]), new Uint8Array([3])]), 10);
  expect(buf && [...buf]).toEqual([1, 2, 3]);
});

test("stops reading and returns null once the cap is passed", async () => {
  const buf = await readBodyAtMost(stream([new Uint8Array(8), new Uint8Array(8)]), 10);
  expect(buf).toBeNull();
});
