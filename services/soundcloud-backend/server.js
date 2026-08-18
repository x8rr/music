import express from 'express';
import { spawn } from 'child_process';
import { Readable } from 'stream';
import { createHash } from 'crypto';
import { existsSync, createReadStream, createWriteStream, statSync, unlinkSync, mkdirSync, promises as fsp } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const app = express();

const PORT  = process.env.PORT || 8081;
const YTDLP = existsSync(join(__dirname, 'yt-dlp')) ? join(__dirname, 'yt-dlp') : 'yt-dlp';

const AUDIO_DIR = process.env.SC_AUDIO_DIR || '/tmp/zodiac-sc-audio';
mkdirSync(AUDIO_DIR, { recursive: true });
const _audioCache  = new Map();
const _streamCache = new Map();
const _STREAM_TTL  = 90 * 60 * 1000;

// --- disk cache reaper ------------------------------------------------------
// Cherri races every source at once, so a lot of tracks get downloaded here and
// then abandoned when a faster source wins. Without this the cache only ever
// grows. Evict by idle time first, then by total size (oldest access first)
// until we're back under budget.
const _CACHE_MAX_BYTES   = (Number(process.env.SC_CACHE_MAX_MB)   || 2048) * 1024 * 1024;
const _CACHE_MAX_IDLE_MS = (Number(process.env.SC_CACHE_MAX_IDLE_MIN) || 360) * 60 * 1000;
const _CACHE_SWEEP_MS    = (Number(process.env.SC_CACHE_SWEEP_MIN)    ||  15) * 60 * 1000;

/** Files being written right now must never be reaped — the entry is only
 * marked done once yt-dlp exits cleanly. */
function _inFlightKeys() {
	const keys = new Set();
	for (const [k, e] of _audioCache) if (!e.done) keys.add(k);
	return keys;
}

async function _reapCache() {
	let names;
	try { names = await fsp.readdir(AUDIO_DIR); } catch { return; }

	const inFlight = _inFlightKeys();
	const now = Date.now();
	const files = [];

	for (const name of names) {
		if (!name.endsWith('.audio')) continue;
		const key = name.slice(0, -'.audio'.length);
		if (inFlight.has(key)) continue;
		const path = `${AUDIO_DIR}/${name}`;
		try {
			const st = await fsp.stat(path);
			// atime, not mtime: a track that keeps getting replayed is still
			// worth keeping even though it was written once and never again.
			files.push({ path, key, size: st.size, atime: st.atimeMs });
		} catch {}
	}

	let freed = 0, removed = 0;
	const drop = async (f) => {
		try {
			await fsp.unlink(f.path);
			_audioCache.delete(f.key);
			freed += f.size;
			removed++;
			return true;
		} catch { return false; }
	};

	const survivors = [];
	for (const f of files) {
		if (now - f.atime > _CACHE_MAX_IDLE_MS) await drop(f);
		else survivors.push(f);
	}

	let total = survivors.reduce((n, f) => n + f.size, 0);
	if (total > _CACHE_MAX_BYTES) {
		survivors.sort((a, b) => a.atime - b.atime);
		for (const f of survivors) {
			if (total <= _CACHE_MAX_BYTES) break;
			if (await drop(f)) total -= f.size;
		}
	}

	if (removed) {
		console.log(`[reaper] removed ${removed} file(s), freed ${(freed / 1048576).toFixed(1)}MB, ${(total / 1048576).toFixed(1)}MB remaining`);
	}
}

_reapCache();
setInterval(_reapCache, _CACHE_SWEEP_MS).unref();

function _streamCacheGet(url) {
	const e = _streamCache.get(url);
	return e && Date.now() - e.ts < _STREAM_TTL ? e.v : null;
}
function _streamCacheSet(url, v) {
	_streamCache.set(url, { v, ts: Date.now() });
	if (_streamCache.size > 120) {
		const now = Date.now();
		for (const [k, e] of _streamCache) if (now - e.ts > _STREAM_TTL) _streamCache.delete(k);
	}
}

function _sseStart(res) {
	res.setHeader('Content-Type', 'text/event-stream');
	res.setHeader('Cache-Control', 'no-cache');
	res.setHeader('Access-Control-Allow-Origin', '*');
	res.flushHeaders();
}

function _ytdlp(args, timeout = 20000) {
	return new Promise((resolve, reject) => {
		const out = [], err = [];
		const p = spawn(YTDLP, args, { stdio: ['ignore', 'pipe', 'pipe'] });
		p.stdout.on('data', d => out.push(d));
		p.stderr.on('data', d => err.push(d));
		const t = setTimeout(() => { p.kill(); reject(new Error('yt-dlp timeout')); }, timeout);
		p.on('close', code => {
			clearTimeout(t);
			if (code === 0 || out.length) resolve(Buffer.concat(out).toString('utf8'));
			else reject(new Error(Buffer.concat(err).toString('utf8').slice(0, 200) || `exit ${code}`));
		});
		p.on('error', e => { clearTimeout(t); reject(e); });
	});
}

async function _serveAudioCached(req, res, cacheKey, ytdlpArgs, contentType = 'audio/mpeg') {
	const cacheFile = `${AUDIO_DIR}/${cacheKey}.audio`;

	const serveFile = () => {
		try {
			const st = statSync(cacheFile);
			const total = st.size;
			// Bump atime explicitly. Most mounts are relatime, where a read only
			// moves atime if it's already a day stale — so the reaper would
			// otherwise see actively-played tracks as idle and evict them.
			fsp.utimes(cacheFile, new Date(), st.mtime).catch(() => {});
			const range = req.headers.range;
			res.setHeader('Content-Type', contentType);
			res.setHeader('Cache-Control', 'max-age=86400');
			res.setHeader('Access-Control-Allow-Origin', '*');
			res.setHeader('Accept-Ranges', 'bytes');
			if (range) {
				const [s, e] = range.replace(/bytes=/, '').split('-');
				const start = parseInt(s, 10) || 0;
				const end   = e ? parseInt(e, 10) : total - 1;
				res.status(206);
				res.setHeader('Content-Range', `bytes ${start}-${end}/${total}`);
				res.setHeader('Content-Length', end - start + 1);
				const rs = createReadStream(cacheFile, { start, end });
				rs.pipe(res);
				res.on('close', () => rs.destroy());
			} else {
				res.setHeader('Content-Length', total);
				createReadStream(cacheFile).pipe(res);
			}
		} catch { if (!res.headersSent) res.status(502).end(); else res.end(); }
	};

	const cached = _audioCache.get(cacheKey);
	if (cached?.done) { serveFile(); return; }
	try { if (statSync(cacheFile).size > 10000) { _audioCache.set(cacheKey, { done: true, promise: null }); serveFile(); return; } } catch {}
	if (cached?.promise) {
		try { await cached.promise; } catch { if (!res.headersSent) res.status(502).end(); return; }
		serveFile(); return;
	}

	let resolveCache, rejectCache;
	const promise = new Promise((rv, rj) => { resolveCache = rv; rejectCache = rj; });
	promise.catch(() => {});
	_audioCache.set(cacheKey, { done: false, promise });

	const proc = spawn(YTDLP, ytdlpArgs);
	const fileOut = createWriteStream(cacheFile);
	proc.stdout.pipe(fileOut);
	proc.stderr.on('data', () => {});

	const downloadDone = new Promise((resolve, reject) => {
		let procOk = false, fileOk = false;
		const check = () => { if (procOk && fileOk) resolve(); };
		proc.on('close', code => {
			if (code !== 0) { reject(new Error(`yt-dlp exited ${code}`)); return; }
			procOk = true; check();
		});
		proc.on('error', reject);
		fileOut.on('finish', () => { fileOk = true; check(); });
		fileOut.on('error', reject);
	});

	try {
		await downloadDone;
		_audioCache.set(cacheKey, { done: true, promise: null });
		resolveCache();
		if (!res.destroyed) serveFile();
	} catch (err) {
		fileOut.destroy();
		try { unlinkSync(cacheFile); } catch {}
		_audioCache.delete(cacheKey);
		rejectCache(err);
		if (!res.destroyed && !res.headersSent) res.status(502).end();
	}
}

// GET /api/sc/stream?url=<soundcloud track url>
// Downloads (and disk-caches) the best available audio for a SoundCloud
// track and serves it back with HTTP range support.
app.get('/api/sc/stream', async (req, res) => {
	const trackUrl = (req.query.url || '').trim();
	if (!trackUrl || !/soundcloud\.com\//.test(trackUrl)) return res.status(400).end();
	const cacheKey = createHash('md5').update('sc:' + trackUrl).digest('hex');
	await _serveAudioCached(req, res, cacheKey, [
		'-f', 'bestaudio[protocol!~=m3u8][ext=mp3]/bestaudio[protocol!~=m3u8]',
		'-o', '-', '--no-warnings', '--quiet', trackUrl,
	], 'audio/mpeg');
});

// POST /api/music/prewarm { url } — warms the direct-stream-URL cache for a
// SoundCloud track ahead of time so /api/music/stream resolves instantly.
app.post('/api/music/prewarm', express.json(), (req, res) => {
	const url = (req.body?.url || '').trim();
	if (!url || !/^https?:\/\/(www\.)?soundcloud\.com\//i.test(url)) return res.status(400).end();
	res.json({ ok: true });
	if (_streamCacheGet(url)) return;
	_ytdlp(['--get-url', '-f', 'bestaudio/best', '--no-warnings', '--quiet', url], 25000)
		.then(raw => { const v = raw.trim().split('\n')[0]; if (v?.startsWith('http')) _streamCacheSet(url, v); })
		.catch(() => {});
});

// GET /api/music/search?q=<query>&limit=<n> — SSE stream of SoundCloud
// search results (scsearch via yt-dlp, flat-playlist).
app.get('/api/music/search', (req, res) => {
	const q     = (req.query.q   || '').trim().slice(0, 200);
	const limit = Math.min(parseInt(req.query.limit) || 20, 40);

	res.setHeader('Content-Type', 'text/event-stream');
	res.setHeader('Cache-Control', 'no-cache');
	res.setHeader('Connection', 'keep-alive');
	res.setHeader('Access-Control-Allow-Origin', '*');
	res.flushHeaders();

	if (!q) { res.write('data: [DONE]\n\n'); return res.end(); }

	const args = [`scsearch${limit}:${q}`, '--flat-playlist', '--dump-json', '--no-warnings', '--quiet'];

	const proc = spawn(YTDLP, args);
	let buf = '';

	// --flat-playlist leaves `thumbnail` null, but the sized variants are all
	// there in `thumbnails`. Prefer 500x500 artwork, else the largest one with
	// a known width ("original" has no dimensions and can be multi-MB).
	const pickThumb = t => {
		if (t.thumbnail) return t.thumbnail;
		const list = Array.isArray(t.thumbnails) ? t.thumbnails : [];
		const exact = list.find(x => x.id === 't500x500');
		if (exact?.url) return exact.url;
		const sized = list.filter(x => x.url && x.width).sort((a, b) => b.width - a.width);
		return sized[0]?.url || '';
	};

	const sendTrack = line => {
		if (!line.trim()) return;
		try {
			const t = JSON.parse(line);
			const item = {
				url:       t.webpage_url || t.url,
				stream:    '',
				title:     t.title,
				artist:    t.uploader || t.channel || '',
				artistUrl: t.uploader_url || '',
				duration:  t.duration || 0,
				thumb:     pickThumb(t),
				src:       'sc',
			};
			res.write(`data: ${JSON.stringify(item)}\n\n`);
		} catch {}
	};

	proc.stdout.on('data', chunk => {
		buf += chunk.toString();
		const lines = buf.split('\n');
		buf = lines.pop() || '';
		lines.forEach(sendTrack);
	});

	const finish = () => { if (buf.trim()) sendTrack(buf); res.write('data: [DONE]\n\n'); res.end(); };
	proc.stdout.on('end', finish);
	proc.on('error', finish);
	res.on('close', () => { try { proc.kill(); } catch {} });
});

// GET /api/music/stream?url=<soundcloud track url>&stream=<optional known stream url>
// Resolves the track's HLS (m3u8) playlist and rewrites segment URIs to be
// proxied through /api/music/seg (keeps CDN auth/headers server-side).
app.get('/api/music/stream', async (req, res) => {
	const trackUrl  = (req.query.url    || '').trim();
	const preStream = (req.query.stream || '').trim();

	const isSC = /^https?:\/\/(www\.)?soundcloud\.com\//i.test(trackUrl)
						|| /sndcdn\.com|soundcloud\.cloud/i.test(preStream);
	if (!isSC) return res.status(400).send('Invalid URL');

	try {
		let m3u8Url = _streamCacheGet(trackUrl) || preStream || '';
		if (!m3u8Url) {
			const raw = await _ytdlp(['--get-url', '-f', 'bestaudio/best', '--no-warnings', '--quiet', trackUrl], 25000);
			m3u8Url = raw.trim().split('\n')[0];
			if (m3u8Url?.startsWith('http')) _streamCacheSet(trackUrl, m3u8Url);
		}
		if (!m3u8Url?.startsWith('http')) return res.status(502).send('No SC stream URL');
		const m3u8Res = await fetch(m3u8Url, { signal: AbortSignal.timeout(10000) });
		if (!m3u8Res.ok) return res.status(502).send('M3U8 fetch failed');
		let m3u8 = await m3u8Res.text();
		m3u8 = m3u8
			.replace(/URI="(https?:\/\/[^"]+)"/g, (_, u) => `URI="/api/music/seg?u=${encodeURIComponent(u)}"`)
			.replace(/^(https?:\/\/.+)$/gm, u => `/api/music/seg?u=${encodeURIComponent(u)}`);
		res.setHeader('Content-Type', 'application/x-mpegurl');
		res.setHeader('Cache-Control', 'no-store');
		res.setHeader('Access-Control-Allow-Origin', '*');
		return res.send(m3u8);
	} catch (e) {
		if (!res.headersSent) res.status(502).send('Stream error: ' + e.message);
	}
});

// GET /api/music/seg?u=<cdn segment url> — allowlisted proxy for the actual
// HLS audio segments referenced by the rewritten m3u8 above.
const SC_CDN = ['sndcdn.com', 'soundcloud.cloud', 'soundcloud.com', 'akamaized.net'];
app.get('/api/music/seg', async (req, res) => {
	const u = (req.query.u || '').trim();
	if (!u || !/^https?:\/\//.test(u)) return res.status(400).send('Bad URL');
	try {
		const host = new URL(u).hostname;
		if (!SC_CDN.some(d => host === d || host.endsWith('.' + d))) {
			return res.status(403).send('Forbidden CDN');
		}
		const r = await fetch(u, { signal: AbortSignal.timeout(15000) });
		if (!r.ok) return res.status(r.status).send('Segment error');
		res.setHeader('Content-Type', r.headers.get('content-type') || 'application/octet-stream');
		res.setHeader('Cache-Control', 'public, max-age=3600');
		res.setHeader('Access-Control-Allow-Origin', '*');
		const readable = Readable.fromWeb(r.body);
		readable.on('error', () => res.destroy());
		res.on('close', () => readable.destroy());
		readable.pipe(res);
	} catch (e) {
		if (!res.headersSent) res.status(502).send(e.message);
	}
});

// GET /api/music/status — quick health check (confirms yt-dlp is runnable).
app.get('/api/music/status', async (_req, res) => {
	try {
		const v = await _ytdlp(['--version'], 5000);
		res.json({ ok: true, version: v.trim() });
	} catch { res.json({ ok: false }); }
});

// Bind loopback by default. /api/music/seg is an allowlisted but otherwise
// open proxy to the SoundCloud CDNs, so anyone who can reach this port can use
// it. Cherri talks to it from the same host; set SC_HOST=0.0.0.0 only if the
// backend genuinely lives on another box, and firewall it if you do.
const HOST = process.env.SC_HOST || "127.0.0.1";

app.listen(PORT, HOST, () => {
	console.log(`SoundCloud backend listening on http://${HOST}:${PORT}`);
});
