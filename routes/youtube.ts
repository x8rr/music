import { FastifyInstance, FastifyReply } from "fastify";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import yts from "yt-search";
import { create as createYoutubeDl } from "youtube-dl-exec";





const youtubeDl = createYoutubeDl(process.env.YTDLP_PATH || "yt-dlp");

const ytsSearch = yts as unknown as (
  q: string | { query: string },
) => Promise<any>;



function channelVideosUrl(channelId: string): string {
  if (channelId.startsWith("UC"))
    return `https://www.youtube.com/channel/${channelId}/videos`;
  
  return `https://www.youtube.com/${channelId}/videos`;
}

function formatSubCount(n?: number): string | undefined {
  if (!n || n <= 0) return undefined;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}



async function fetchChannelUploads(
  channelId: string,
  count: number,
): Promise<{
  name?: string;
  id: string;
  url?: string;
  subscriberCount?: string;
  videoCount?: number;
  videos: any[];
}> {
  const info = (await youtubeDl(channelVideosUrl(channelId), {
    flatPlaylist: true,
    dumpSingleJson: true,
    playlistEnd: count,
  })) as any;

  const entries: any[] = Array.isArray(info?.entries) ? info.entries : [];
  const videos = entries
    .filter((e) => e?.id)
    .map((e: any) => ({
      id: e.id,
      title: e.title ?? "",
      description: e.description ?? "",
      timestamp: e.timestamp ?? "",
      duration: e.duration ?? 0,
      age: e.upload_date ?? "",
      views: e.view_count ?? null,
      thumbnail: `/api/yt/thumbnail/${e.id}`,
      mediaUrl: `/api/yt/id/${e.id}`,
    }));

  return {
    name: info?.channel ?? info?.uploader,
    id: info?.channel_id ?? channelId,
    url: info?.channel_url ?? info?.uploader_url,
    subscriberCount: formatSubCount(info?.channel_follower_count),
    videoCount:
      typeof info?.playlist_count === "number" && info.playlist_count > 0
        ? info.playlist_count
        : undefined,
    videos,
  };
}

/**
 * Proxy a remote image, streamed, with the upstream bound to the client.
 *
 * The previous implementation piped an axios stream into res.raw and settled a
 * promise on 'end'/'error'. Neither fires when the *client* goes away, so every
 * abandoned request — and thumbnail requests are abandoned constantly, because
 * lists render dozens and users scroll past them — left an upstream response
 * buffering into memory nobody would ever read. Measured at ~220KB leaked per
 * request, which is most of an 11GB overnight climb.
 *
 * Aborting on `close` is what actually frees it; the rest is caching so the
 * same thumbnail is not refetched on every render.
 */
async function proxyImage(
  req: { raw: { on(event: "close", cb: () => void): void } },
  reply: FastifyReply,
  url: string,
) {
  const abort = new AbortController();
  req.raw.on("close", () => abort.abort());

  let upstream: Response;
  try {
    upstream = await fetch(url, { signal: abort.signal });
  } catch {
    return reply.status(502).send({ error: "Upstream fetch failed" });
  }
  if (!upstream.ok || !upstream.body) {
    upstream.body?.cancel().catch(() => {});
    return reply.status(upstream.status === 404 ? 404 : 502).send({
      error: "Image unavailable",
    });
  }

  reply.header(
    "content-type",
    upstream.headers.get("content-type") || "image/jpeg",
  );
  // Thumbnails are immutable for a given id; without this the client re-asked
  // on every render, which is what made the leak above fire so often.
  reply.header("cache-control", "public, max-age=86400");
  return reply.send(
    Readable.fromWeb(upstream.body as unknown as NodeReadableStream),
  );
}

export async function youtubeRoutes(fastify: FastifyInstance) {
  fastify.get("/api/yt/thumbnail/:id", async (req, res) => {
    const { id: videoId } = req.params as { id: string };
    // The id lands in an upstream URL, so it is held to YouTube's own format
    // rather than passed through.
    if (!/^[A-Za-z0-9_-]{11}$/.test(videoId)) {
      return res.status(400).send({ error: "Invalid video id" });
    }
    return proxyImage(
      req,
      res,
      `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
    );
  });

  fastify.get("/api/yt/search", async (req, res) => {
    const { q: query } = req.query as { q: string };
    if (!query) {
      res.status(400).send({ error: "You must define a query parameter (q)" });
    }
    try {
      const results = await ytsSearch(query);
      const videos = results.videos.map((v: any) => ({
        id: v.videoId,
        title: v.title,
        description: v.description,
        timestamp: v.timestamp,
        duration: v.duration.seconds,
        age: v.ago,
        views: v.views,
        thumbnail: `/api/yt/thumbnail/${v.videoId}`,
        mediaUrl: `/id/${v.videoId}`,
        author: {
          name: v.author.name,
          url: v.author.url,
          channelId: v.author.url.split("/").pop(),
          proxyUrl: `/media/account/${v.author.url.split("/").pop()}`,
        },
      }));
      res.send({
        query,
        results: videos,
      });
    } catch (e: unknown) {
      res
        .status(500)
        .send({ error: "search failed", details: (e as Error).message });
    }
  });

  fastify.get("/api/yt/id/:id", async (req, res) => {
    const { id: videoId } = req.params as { id: string };
    const videoUrl = `https://www.youtube.com/watch?v=${videoId}`;
    try {
      const info = (await youtubeDl(videoUrl, {
        format: "best",
        getUrl: true,
      } as Record<string, string | boolean>)) as unknown as string;

      const cdnUrl = info.trim();
      const range = req.headers.range;

      // Video, so an abandoned request holds far more than a thumbnail: a
      // seek or a closed tab leaves the CDN pushing megabytes into a buffer
      // with no reader. Bind the upstream to the client connection.
      const abort = new AbortController();
      req.raw.on("close", () => abort.abort());

      const upstream = await fetch(cdnUrl, {
        headers: {
          ...(range ? { range } : {}),
          "user-agent": "Mozilla/5.0",
          referer: "https://www.youtube.com",
        },
        signal: abort.signal,
      });

      res.code(upstream.status);
      res.header(
        "content-type",
        upstream.headers.get("content-type") || "video/mp4",
      );
      res.header("accept-ranges", "bytes");
      res.header("cache-control", "no-store");

      const contentLength = upstream.headers.get("content-length");
      const contentRange = upstream.headers.get("content-range");
      if (contentLength) res.header("content-length", contentLength);
      if (contentRange) res.header("content-range", contentRange);

      if (!upstream.body) return res.send();
      return res.send(
        Readable.fromWeb(upstream.body as unknown as NodeReadableStream),
      );
    } catch (e: unknown) {
      res
        .status(500)
        .send({ error: "Stream failed", details: (e as Error).message });
    }
  });

  fastify.get("/api/yt/media/account/:id", async (req, res) => {
    const { id: channelId } = req.params as { id: string };
    try {
      const results = await ytsSearch({ query: channelId });
      const channel = results.channels[0];
      if (!channel?.image) {
        return res.status(404).send("Channel icon not found");
      }
      return await proxyImage(req, res, channel.image);
    } catch {
      res.status(500).send("Error proxying profile icon");
    }
  });

  fastify.get("/api/yt/account/:id", async (req, res) => {
    const { id: channelId } = req.params as { id: string };
    try {
      const ch = await fetchChannelUploads(channelId, 100);
      return res.send({
        name: ch.name,
        id: ch.id,
        url: ch.url,
        profileIcon: `/api/yt/media/account/${channelId}`,
        subscriberCount: ch.subscriberCount,
        videoCount: ch.videoCount,
        topVideos: ch.videos.slice(0, 100),
      });
    } catch {
      
      
      try {
        const results = await ytsSearch({ query: channelId });
        const channel = results.channels[0];
        if (!channel)
          return res.status(404).send({ error: "Channel not found" });
        const videos = results.videos.map((v: any) => ({
          id: v.videoId,
          title: v.title,
          description: v.description,
          timestamp: v.timestamp,
          duration: v.duration.seconds,
          age: v.ago,
          views: v.views,
          thumbnail: `/api/yt/thumbnail/${v.videoId}`,
          mediaUrl: `/api/yt/id/${v.videoId}`,
        }));
        return res.send({
          name: channel.name,
          id: channelId,
          url: channel.url,
          profileIcon: `/api/yt/media/account/${channelId}`,
          subscriberCount: channel.subCountLabel,
          videoCount: channel.videoCount > 0 ? channel.videoCount : undefined,
          topVideos: videos.slice(0, 100),
        });
      } catch {
        return res.status(500).send({ error: "Failed to fetch account info" });
      }
    }
  });

  fastify.get("/api/yt/account/:id/videos", async (req, res) => {
    const { id: channelId } = req.params as { id: string };
    const { limit: limitStr, offset: offsetStr } = req.query as {
      limit?: string;
      offset?: string;
    };
    const limit = parseInt(limitStr ?? "20");
    const offset = parseInt(offsetStr ?? "0");
    try {
      
      const ch = await fetchChannelUploads(channelId, offset + limit);
      res.send({
        channelId,
        totalFound: ch.videos.length,
        limit,
        offset,
        videos: ch.videos.slice(offset, offset + limit),
      });
    } catch {
      res.status(500).send({ error: "Failed to fetch creator videos" });
    }
  });
  fastify.get("/api/yt/comments/:id", async (req, res) => {
    const { id: videoId } = req.params as { id: string };
    const { limit: limitStr = "20" } = req.query as { limit?: string };
    const limit = Math.min(parseInt(limitStr), 100);
    const videoUrl = `https://www.youtube.com/watch?v=${videoId}`;
    try {
      const info = (await youtubeDl(videoUrl, {
        writeComments: true,
        dumpSingleJson: true,
        skipDownload: true,
        extractorArgs: "youtube:comment_sort=top",
      } as Record<string, string | boolean>)) as unknown as any;

      const raw: any[] = info.comments || [];
      const comments = raw
        .filter((c) => !c.parent || c.parent === "root")
        .slice(0, limit)
        .map((c) => ({
          id: c.id,
          text: c.text,
          author: c.author,
          authorThumbnail: c.author_thumbnail || null,
          likes: c.like_count || 0,
          published: c.timestamp
            ? new Date(c.timestamp * 1000).toLocaleDateString("en-US", {
                year: "numeric",
                month: "short",
                day: "numeric",
              })
            : c.time_text || "",
          isHearted: c.is_favorited || false,
          isPinned: c.is_pinned || false,
        }));

      res.send({ videoId, total: raw.length, comments });
    } catch (e: unknown) {
      res
        .status(500)
        .send({
          error: "Failed to fetch comments",
          details: (e as Error).message,
        });
    }
  });
}
