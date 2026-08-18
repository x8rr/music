# soundcloud-backend

Standalone yt-dlp-backed SoundCloud service. Cherri talks to it as the `scdlp`
stream source (see `server/routes/music.ts`).

It exists as a separate fallback from the built-in `soundcloud` source because
that one goes through SoundCloud's api-v2 with a scraped `client_id`, which
breaks whenever the scrape stops working or gets rate-limited. This one goes
through yt-dlp instead, so the two fail independently.

## Running

```sh
cd services/soundcloud-backend
npm install
curl -L -o yt-dlp https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp
chmod +x yt-dlp
npm start          # listens on :8081
```

Falls back to whatever `yt-dlp` is on `PATH` if the local binary is absent.

## Cherri-side config

| Env var | Default | Meaning |
| --- | --- | --- |
| `SC_DLP_BASES` | `http://127.0.0.1:8081` | Comma-separated bases; overrides `scdlpBases` in `server/config/music-sources.json` |
| `SC_DLP_TIMEOUT_MS` | `8000` | Search/resolve timeout |
| `SC_DLP_PLAYABILITY_TIMEOUT_MS` | `25000` | How long the playability probe waits for a cold-cache download |

Leaving `scdlpBases` empty disables the source cleanly — it's filtered out of
the stream race and the health ping rather than failing on every track.

## Endpoints used by Cherri

- `GET /api/music/search?q&limit` — SSE track list, used for search and for
  matching an artist/title to a SoundCloud permalink
- `GET /api/sc/stream?url` — downloads + disk-caches the track, serves it with
  HTTP range support (this is what ends up in the player)
- `POST /api/music/prewarm {url}` — starts the download early
- `GET /api/music/status` — health check, used by the source ping

## Disk cache

Tracks are cached to `SC_AUDIO_DIR` (default `/tmp/zodiac-sc-audio`). A reaper
sweeps it on an interval: first dropping anything idle past the cutoff, then —
if still over budget — evicting least-recently-accessed files until it fits.
Files still being downloaded are never evicted.

| Env var | Default | Meaning |
| --- | --- | --- |
| `SC_AUDIO_DIR` | `/tmp/zodiac-sc-audio` | Cache location |
| `SC_CACHE_MAX_MB` | `2048` | Size budget |
| `SC_CACHE_MAX_IDLE_MIN` | `360` | Evict anything untouched this long |
| `SC_CACHE_SWEEP_MIN` | `15` | Sweep interval (also runs once at startup) |

This matters more than it looks: Cherri races all sources at once, so tracks
get downloaded here and then abandoned whenever a faster source wins.

## Operational notes

- `/api/music/seg` is an open proxy to `sndcdn.com`, `soundcloud.cloud`,
  `soundcloud.com` and `akamaized.net` for anyone who can reach the port. Bind
  it to localhost or firewall it; don't expose it publicly.
- DRM-protected tracks (a lot of major-label uploads) fail here with a 502.
  That's expected — the race just resolves from another source.
