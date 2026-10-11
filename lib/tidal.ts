



const CLIENT_ID = process.env.TIDAL_CLIENT_ID || "txNoH4kkV41MfH25";
const CLIENT_SECRET =
  process.env.TIDAL_CLIENT_SECRET ||
  "dQjy0MinCEvxi1O4UmxvxWnDjt4cgHBPw8ll6nYBk98=";
const COUNTRY = process.env.TIDAL_COUNTRY || "US";


// The old default proxy (td.if-it-runs-ship-it.lol) no longer resolves, so
// every call here failed on DNS and search leaned on a single hifi mirror.
// api.tidal.com answers the prod box directly (checked 2026-09-29).
const TIDAL_PROXY = process.env.TIDAL_PROXY || "https://api.tidal.com";

const TIMEOUT_MS = Number(process.env.TIDAL_TIMEOUT_MS) || 8000;

let cachedToken: { value: string; exp: number } | null = null;
let tokenJob: Promise<string> | null = null;

// One token request at a time, and bounded: every search waits on Tidal for
// its albums, so a hung token fetch used to hang them all.
function getToken(): Promise<string> {
  if (cachedToken && Date.now() < cachedToken.exp) return Promise.resolve(cachedToken.value);
  tokenJob ??= fetchToken().finally(() => (tokenJob = null));
  return tokenJob;
}

async function fetchToken(): Promise<string> {
  const res = await fetch("https://auth.tidal.com/v1/oauth2/token", {
    signal: AbortSignal.timeout(TIMEOUT_MS),
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
    
    exp: Date.now() + (data.expires_in ?? 3600) * 1000 - 60_000,
  };
  return cachedToken.value;
}

export async function tidalApi<T>(
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


export async function tidalSearchTracks(
  query: string,
  limit: number,
): Promise<{ items?: unknown[]; totalNumberOfItems?: number }> {
  return tidalApi("/v1/search/tracks", { query, limit });
}


export async function tidalPlaylistTracks(
  uuid: string,
  limit: number,
): Promise<{ items?: unknown[] }> {
  return tidalApi(`/v1/playlists/${uuid}/tracks`, { limit });
}
