# Zone Occupancy Dashboard (frontend)

A Vite + React + TypeScript live dashboard for the IoT Video Event Stream Analytics backend (`src/dashboard/api.py`). It connects to the backend's existing `WS /ws` endpoint and renders per-zone occupancy, rolling stats, live charts, and anomaly alerts in real time.

See the repository root [`README.md`](../README.md#live-dashboard) -- "Live Dashboard" section -- for a screenshot, an explanation of what it shows, and instructions to run it (via Docker Compose or standalone with `npm run dev`).

## Local development

```bash
npm install
npm run dev
```

## Build / type-check

```bash
npm run build          # tsc -b && vite build
npx tsc --noEmit -p tsconfig.app.json
```
