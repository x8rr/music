import Fastify from "fastify";
import cors from "@fastify/cors";
import staticPlugin from "@fastify/static";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { musicRoutes } from "./routes/music";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fastify = Fastify({ logger: { level: "info" } });

async function startServer() {
  await fastify.register(cors, { origin: true });
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
    console.log("x8music spinning safely on http://localhost:" + usePort);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

startServer();
