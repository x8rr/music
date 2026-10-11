import Fastify from "fastify";
import cors from "@fastify/cors";
import rateLimit from "@fastify/rate-limit";
import staticPlugin from "@fastify/static";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { musicRoutes } from "./routes/music";
import { startCacheSweep } from "./lib/cacheSweep";
import { artCacheDir } from "./lib/artCache";
import { r2 } from "./lib/r2";
import { ytmBase, ytmCacheDir } from "./lib/ytm";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fastify = Fastify({ logger: { level: "info" } });

async function startServer() {
  await fastify.register(cors, { origin: true });
  // Off unless a route asks for it (the artwork proxy does).
  await fastify.register(rateLimit, { global: false });
  await fastify.register(staticPlugin, {
    root: path.join(__dirname, "public"),
  });
  fastify.register(musicRoutes);

  fastify.setNotFoundHandler((req, res) => {
    res
      .code(404)
      .send({
        error: `${req.url} was not found on this server. Check the spelling and try again.`,
      });
  });

  try {
    const usePort = Number(process.env.MUSIC_PORT) || 2010;
    await fastify.listen({ port: usePort, host: "0.0.0.0" });

    const gb = (v: string | undefined, dflt: number) => (Number(v) || dflt) * 1024 ** 3;
    const day = 24 * 60 * 60 * 1000;
    // With R2 behind it (lib/r2.ts) the disk only needs the covers people keep
    // opening. Without it, an evicted cover goes back to a provider that
    // rate-limits us, so the disk stays big.
    startCacheSweep("art", artCacheDir(), { maxBytes: gb(process.env.ART_CACHE_MAX_GB, r2 ? 5 : 20), maxAgeMs: 60 * day });
    startCacheSweep("ytm", ytmCacheDir(), { maxBytes: gb(process.env.YTM_CACHE_MAX_GB, 8), maxAgeMs: 30 * day });

    const base = ytmBase();
    if (base) console.log(`YouTube Music: ${base}`);
    else console.warn("YouTube Music: not configured. Search works, nothing will play until YTM_BASE is set (see .env.example)");
    if (r2) console.log(`R2: ${process.env.R2_BASE} (bucket ${process.env.R2_BUCKET})`);
    else console.warn("R2: not configured. Audio and artwork are kept on local disk only (see .env.example)");
    console.log("x8music spinning safely on http://localhost:" + usePort);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

startServer();
