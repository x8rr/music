/**
 * `response.arrayBuffer()` has no size limit. A Content-Length check is only
 * advisory: chunked responses and dishonest origins bypass it and make Bun
 * grow a native receive buffer until the whole body is available. Read
 * incrementally so an advertised ceiling is a real ceiling; returns null (and
 * cancels the stream) once `maxBytes` is exceeded.
 */
export async function readBodyAtMost(
  body: ReadableStream<Uint8Array>,
  maxBytes: number,
): Promise<Buffer | null> {
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel();
        return null;
      }
      chunks.push(value);
    }
    return Buffer.concat(chunks, total);
  } finally {
    reader.releaseLock();
  }
}
