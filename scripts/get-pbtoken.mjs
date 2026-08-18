import { chromium } from 'playwright';

const URL = 'https://music.octavestreaming.com/';
const KEY = 'octave:pbtoken';

const browser = await chromium.launch();
const page = await browser.newPage();

await page.goto(URL, { waitUntil: 'domcontentloaded', timeout: 60000 });

await page.waitForFunction(
  (key) => localStorage.getItem(key) !== null,
  KEY,
  { timeout: 30000 },
);

const value = await page.evaluate((key) => localStorage.getItem(key), KEY);

// The app used to store a bare token; it now stores a JSON envelope
// {"token","exp","skew"}. Emit one stable machine-readable line either way so
// server/lib/octave.ts can mint expiry from exp/skew when it has them.
let parsed = null;
try {
  parsed = JSON.parse(value);
} catch {
  // Not JSON — the raw value is the token itself.
}
const payload = {
  token: parsed?.token ?? value,
  exp: typeof parsed?.exp === "number" ? parsed.exp : null,
  skew: typeof parsed?.skew === "number" ? parsed.skew : null,
};
console.log(JSON.stringify(payload));

await browser.close();