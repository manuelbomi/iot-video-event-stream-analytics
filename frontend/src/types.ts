/**
 * TypeScript types mirroring the JSON shape produced by the FastAPI + WebSocket
 * dashboard backend in `src/dashboard/api.py` (via `StreamProcessor.get_snapshot`
 * in `src/processor/stream_processor.py` and the `AlertPayload` model in
 * `src/dashboard/api.py`).
 *
 * The backend pushes a *full* snapshot (not a diff) on connect and roughly
 * once a second thereafter over `WS /ws`, and the same shape is available via
 * `GET /api/snapshot`. Keep these in sync with the backend if that shape ever
 * changes.
 */

/** A single zone's most recent raw event, as stored in `StreamProcessor.latest`. */
export interface LatestReading {
  zone: string;
  occupancy: number;
  timestamp: string;
  mean: number;
  std: number;
  count: number;
}

/** Rolling Welford statistics for a zone, as returned by `get_snapshot`. */
export interface ZoneStats {
  count: number;
  mean: number;
  std: number;
  min: number | null;
  max: number | null;
}

/** One time-bucketed heatmap entry (`TimeBucketAggregator.snapshot`). */
export interface HeatmapBucket {
  bucket_start: string;
  avg_occupancy: number;
  max_occupancy: number;
  sample_count: number;
}

/** Per-zone entry inside `snapshot.zones`. */
export interface ZoneSnapshot {
  latest: LatestReading | null;
  stats: ZoneStats;
  heatmap: HeatmapBucket[];
}

/** An anomaly alert, matching `Alert.to_dict()` / the `/alerts` webhook's `AlertPayload`. */
export interface Alert {
  zone: string;
  timestamp: string;
  occupancy: number;
  z_score: number;
  mean: number;
  std: number;
  reason: string;
}

/** The full snapshot broadcast over `WS /ws` and returned by `GET /api/snapshot`. */
export interface DashboardSnapshot {
  zones: Record<string, ZoneSnapshot>;
  alerts: Alert[];
}

/** A single point plotted on a zone's rolling occupancy line chart (client-side only). */
export interface OccupancyPoint {
  time: string;
  occupancy: number;
  isAnomaly: boolean;
}
