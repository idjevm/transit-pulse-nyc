# Transit Pulse NYC — Mock Demo (Vercel)

**DEMO — simulated data, no live MTA feed, no Confluent.**

This is a static mock of the Transit Pulse NYC dashboard, built for a Vercel
deployment with no backend, no domain required.

🏆 #1 Prize Winner: Confluent AI Day NYC 2026 (September 2026)

## What is simulated

Everything on the page runs in the browser:

- ~18 subway trains moving along approximate NYC line polylines
  (1/2/3, 4/5/6, 7, A/C/E, B/D/F/M, N/Q/R/W, G, J, L) in real MTA colors,
  and ~24 buses moving along short street polylines in
  Manhattan / Brooklyn / Queens.
- Top-bar stats, simulated NYC weather, and crowd surges.
- Bunching / gap alerts, predictive headway forecasts, and AI dispatcher
  recommendations (HOLD TRAIN, GAP FILL, SKIP STOP, MONITOR) with a
  dispatcher note and a rider-facing message.
- Next arrivals board (8 rows) with ETAs counting down.
- AI agent tabs (Rider Advisor, Operator Prediction, Route Designer)
  with canned mock answers grounded in the simulated state. The Route
  Designer draws a cyan dashed proposed route on the map.
- Controls: Subway / Bus toggles, route filter, route-lines toggle,
  Inject Bunching (Times Sq) / Inject Gap (125 St) + Clear,
  Pause / Resume, Reset.

## What is NOT here

- No Confluent, no Kafka / Flink, no live MTA GTFS-Realtime feed.
- No backend, no `/api/*` calls, no WebSocket, no API keys.
- No real vehicle positions, arrivals, or AI model calls.
- Route polylines are approximate, not GTFS geometry.

The only external requests are the Leaflet 1.9.4 CDN (CSS + JS) and
CARTO dark map tiles.

## Deploy on Vercel

No build step. Deploy this folder as a static site:

- Vercel dashboard: Add New → Project → import this folder/repo,
  framework preset: Other, no build command, output directory: `.`
- Or with the Vercel CLI, from this folder: `vercel` (then `vercel --prod`)

The site works at the default `*.vercel.app` URL; no custom domain is needed.

## Files

- `index.html` — single self-contained page (inline CSS + JS)
- `vercel.json` — static config, no build command
- `README.md` — this file
