// In-process Tidal adapter — a self-hosted "HiFi API" so Tidal search and
// streaming don't depend on the flaky public instances. Uses the public
// client-credentials app (same as Monochrome) routed through a region proxy,
// which is enough for catalog search and FLAC playbackinfo manifests.
const CLIENT_ID = process.env.TIDAL_CLIENT_ID || "txNoH4kkV41MfH25";
const CLIENT_SECRET =
  process.env.TIDAL_CLIENT_SECRET ||
  "dQjy0MinCEvxi1O4UmxvxWnDjt4cgHBPw8ll6nYBk98=";
const COUNTRY = process.env.TIDAL_COUNTRY || "US";
// api.tidal.com geo-blocks datacenter IPs; the proxy is what lets the
// client-credentials token reach playbackinfo (Monochrome uses the same one).
const TIDAL_PROXY =
  process.env.TIDAL_PROXY || "https://td.if-it-runs-ship-it.lol/api";

const TIMEOUT_MS = Number(process.env.TIDAL_TIMEOUT_MS) || 8000;

let cachedToken: { value: string; exp: number } | null = null;

async function getToken(): Promise<string> {
  if (cachedToken && Date.now() < cachedToken.exp) return cachedToken.value;
  const res = await fetch("https://auth.tidal.com/v1/oauth2/token", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization:
        "Basic " +
        Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString("base64"),
    },
    body: new URLSearchParams({
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      grant_type: "client_credentials",
    }),
  });
  if (!res.ok) throw new Error(`Tidal token failed: ${res.status}`);
  const data = (await res.json()) as { access_token: string; expires_in?: number };
  cachedToken = {
    value: data.access_token,
    // refresh a minute early
    exp: Date.now() + (data.expires_in ?? 3600) * 1000 - 60_000,
  };
  return cachedToken.value;
}

async function tidalApi<T>(
  path: string,
  params: Record<string, string | number | undefined> = {},
): Promise<T> {
  const token = await getToken();
  const u = new URL(`https://api.tidal.com${path}`);
  u.searchParams.set("countryCode", COUNTRY);
  for (const [k, v] of Object.entries(params))
    if (v !== undefined) u.searchParams.set(k, String(v));
  const url = u.toString().replace("https://api.tidal.com", TIDAL_PROXY);

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
      signal: ctrl.signal,
    });
    if (!res.ok) throw new Error(`Tidal ${path} → ${res.status}`);
    return (await res.json()) as T;
  } finally {
    clearTimeout(t);
  }
}

/** Track search — returns the Tidal v1 shape cherri's toClientTrack expects. */
export async function tidalSearchTracks(
  query: string,
  limit: number,
): Promise<{ items?: unknown[]; totalNumberOfItems?: number }> {
  return tidalApi("/v1/search/tracks", { query, limit });
}

/** Resolve a FLAC DASH manifest for a track (decoded, ready to rewrite). */
export async function tidalTrackManifest(
  id: string,
): Promise<{ rawDash: string; mimeType: string }> {
  const pb = await tidalApi<{ manifest?: string; manifestMimeType?: string }>(
    `/v1/tracks/${id}/playbackinfo`,
    {
      audioquality: "LOSSLESS",
      playbackmode: "STREAM",
      assetpresentation: "FULL",
    },
  );
  if (!pb?.manifest) throw new Error("Tidal playbackinfo returned no manifest");
  const rawDash = Buffer.from(pb.manifest, "base64").toString("utf8");
  return { rawDash, mimeType: pb.manifestMimeType || "application/dash+xml" };
}
