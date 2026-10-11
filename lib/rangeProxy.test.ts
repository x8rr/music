import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import Fastify, { type FastifyInstance } from "fastify";
import {
  parseContentRange,
  parseRangeHeader,
  proxyUpstreamRanged,
} from "./rangeProxy";

const SIZE = 5 * 1024 * 1024 + 123;
const CHUNK = 1024 * 1024;
const body = new Uint8Array(SIZE);
for (let i = 0; i < SIZE; i++) body[i] = (i * 7 + (i >> 8)) & 0xff;

let upstream: ReturnType<typeof Bun.serve>;
let app: FastifyInstance;
let requests: string[] = [];
let mode: "range" | "noRange" | "missing" = "range";

beforeAll(async () => {
  upstream = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch(req) {
      requests.push(req.headers.get("range") ?? "(none)");
      if (mode === "missing") return new Response("nope", { status: 404 });
      const range = req.headers.get("range");
      if (mode === "noRange" || !range) {
        return new Response(body, {
          headers: { "content-type": "audio/test", "content-length": String(SIZE) },
        });
      }
      const m = /^bytes=(\d+)-(\d*)$/.exec(range)!;
      const start = Number(m[1]);
      if (start >= SIZE)
        return new Response(null, { status: 416, headers: { "content-range": `bytes */${SIZE}` } });
      const end = m[2] === "" ? SIZE - 1 : Math.min(Number(m[2]), SIZE - 1);
      return new Response(body.slice(start, end + 1), {
        status: 206,
        headers: {
          "content-type": "audio/test",
          "content-range": `bytes ${start}-${end}/${SIZE}`,
          "content-length": String(end - start + 1),
          "accept-ranges": "bytes",
        },
      });
    },
  });
  app = Fastify({ logger: false });
  app.get("/p", (req, reply) =>
    proxyUpstreamRanged(req, reply, `http://127.0.0.1:${upstream.port}/audio`, {
      chunkBytes: CHUNK,
      responseHeaders: { "cache-control": "no-store" },
    }),
  );
  app.get("/pass", (req, reply) =>
    proxyUpstreamRanged(req, reply, `http://127.0.0.1:${upstream.port}/audio`, {
      passStatuses: [404],
    }),
  );
  await app.listen({ port: 0, host: "127.0.0.1" });
});

afterAll(async () => {
  await app.close();
  upstream.stop(true);
});

function base() {
  const addr = app.server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  return `http://127.0.0.1:${port}`;
}

describe("range header parsing", () => {
  test("accepts the shapes players send", () => {
    expect(parseRangeHeader(undefined)).toBeNull();
    expect(parseRangeHeader("bytes=0-")).toEqual({ start: 0, end: null });
    expect(parseRangeHeader("bytes=10-20")).toEqual({ start: 10, end: 20 });
    expect(parseRangeHeader("bytes=-500")).toBe("unsupported");
    expect(parseRangeHeader("bytes=5-2")).toBe("unsupported");
    expect(parseRangeHeader("bytes=0-1,5-9")).toBe("unsupported");
  });
  test("reads content-range with and without a total", () => {
    expect(parseContentRange("bytes 0-99/1000")).toEqual({ start: 0, end: 99, total: 1000 });
    expect(parseContentRange("bytes 0-99/*")).toEqual({ start: 0, end: 99, total: null });
    expect(parseContentRange("bytes */1000")).toBeNull();
    expect(parseContentRange(null)).toBeNull();
  });
});

describe("proxyUpstreamRanged", () => {
  test("delivers a whole file as a 200 fetched in chunks", async () => {
    requests = [];
    mode = "range";
    const res = await fetch(`${base()}/p`);
    expect(res.status).toBe(200);
    expect(res.headers.get("content-length")).toBe(String(SIZE));
    expect(res.headers.get("content-type")).toBe("audio/test");
    expect(res.headers.get("cache-control")).toBe("no-store");
    const got = new Uint8Array(await res.arrayBuffer());
    expect(got.byteLength).toBe(SIZE);
    expect(Buffer.compare(got, body)).toBe(0);
    // Never more than one chunk in flight, so the upstream saw one request per
    // chunk rather than one for the file.
    expect(requests.length).toBe(Math.ceil(SIZE / CHUNK));
    expect(requests[0]).toBe(`bytes=0-${CHUNK - 1}`);
  });

  test("honours a bounded client range", async () => {
    requests = [];
    const res = await fetch(`${base()}/p`, { headers: { range: "bytes=1000-2999" } });
    expect(res.status).toBe(206);
    expect(res.headers.get("content-range")).toBe(`bytes 1000-2999/${SIZE}`);
    expect(res.headers.get("content-length")).toBe("2000");
    const got = new Uint8Array(await res.arrayBuffer());
    expect(Buffer.compare(got, body.slice(1000, 3000))).toBe(0);
    expect(requests).toEqual(["bytes=1000-2999"]);
  });

  test("honours an open-ended client range across chunks", async () => {
    requests = [];
    const start = SIZE - CHUNK - 10;
    const res = await fetch(`${base()}/p`, { headers: { range: `bytes=${start}-` } });
    expect(res.status).toBe(206);
    expect(res.headers.get("content-range")).toBe(`bytes ${start}-${SIZE - 1}/${SIZE}`);
    const got = new Uint8Array(await res.arrayBuffer());
    expect(Buffer.compare(got, body.slice(start))).toBe(0);
    expect(requests.length).toBe(2);
  });

  test("falls back to one pass when the upstream ignores Range", async () => {
    requests = [];
    mode = "noRange";
    const res = await fetch(`${base()}/p`, { headers: { range: "bytes=0-" } });
    expect(res.status).toBe(200);
    const got = new Uint8Array(await res.arrayBuffer());
    expect(Buffer.compare(got, body)).toBe(0);
    expect(requests.length).toBe(1);
    mode = "range";
  });

  test("answers 502 JSON when the upstream fails before the first byte", async () => {
    mode = "missing";
    const res = await fetch(`${base()}/p`);
    expect(res.status).toBe(502);
    expect(await res.json()).toEqual({ error: "Upstream returned 404" });
    mode = "range";
  });

  test("relays an opted-in upstream status instead of a 502", async () => {
    mode = "missing";
    const res = await fetch(`${base()}/pass`);
    expect(res.status).toBe(404);
    mode = "range";
  });

  test("stops fetching when the client goes away", async () => {
    requests = [];
    const ctrl = new AbortController();
    const res = await fetch(`${base()}/p`, { signal: ctrl.signal });
    const reader = res.body!.getReader();
    await reader.read();
    ctrl.abort();
    await Bun.sleep(300);
    const seen = requests.length;
    await Bun.sleep(300);
    expect(requests.length).toBe(seen);
    expect(seen).toBeLessThan(Math.ceil(SIZE / CHUNK));
  });
});
