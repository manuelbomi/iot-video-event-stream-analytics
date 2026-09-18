import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { Alert, OccupancyPoint, ZoneSnapshot } from "./types";

interface ZoneCardProps {
  zone: string;
  snapshot: ZoneSnapshot;
  history: OccupancyPoint[];
  activeAlert: Alert | null;
}

function severity(alert: Alert | null): "none" | "amber" | "red" {
  if (!alert) return "none";
  return Math.abs(alert.z_score) > 5 ? "red" : "amber";
}

export function ZoneCard({ zone, snapshot, history, activeAlert }: ZoneCardProps) {
  const sev = severity(activeAlert);
  const latest = snapshot.latest;

  return (
    <div className={`zone-card ${sev !== "none" ? `zone-card--${sev}` : ""}`}>
      <div className="zone-card__header">
        <h2 className="zone-card__name">{zone}</h2>
        {sev !== "none" && <span className={`badge badge--${sev}`}>ANOMALY</span>}
      </div>

      <div className="zone-card__metrics">
        <div className="metric">
          <span className="metric__label">Current occupancy</span>
          <span className="metric__value">{latest ? latest.occupancy : "—"}</span>
        </div>
        <div className="metric">
          <span className="metric__label">Rolling mean</span>
          <span className="metric__value">{snapshot.stats.mean}</span>
        </div>
        <div className="metric">
          <span className="metric__label">Rolling std</span>
          <span className="metric__value">{snapshot.stats.std}</span>
        </div>
        <div className="metric">
          <span className="metric__label">Samples seen</span>
          <span className="metric__value">{snapshot.stats.count}</span>
        </div>
      </div>

      <div className="zone-card__chart">
        <ResponsiveContainer width="100%" height={140}>
          <LineChart data={history} margin={{ top: 4, right: 8, bottom: 0, left: -24 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--chart-grid)" />
            <XAxis dataKey="time" tick={{ fontSize: 10, fill: "var(--text-muted)" }} minTickGap={30} />
            <YAxis tick={{ fontSize: 10, fill: "var(--text-muted)" }} width={32} />
            <Tooltip
              contentStyle={{
                background: "var(--surface-2)",
                border: "1px solid var(--border)",
                borderRadius: 6,
                fontSize: 12,
              }}
              labelStyle={{ color: "var(--text-muted)" }}
            />
            <Line
              type="monotone"
              dataKey="occupancy"
              stroke={sev === "red" ? "var(--danger)" : sev === "amber" ? "var(--warning)" : "var(--accent)"}
              strokeWidth={2}
              dot={(props: { cx?: number; cy?: number; index?: number; payload?: OccupancyPoint }) => {
                const { cx, cy, index, payload } = props;
                if (!payload?.isAnomaly || cx === undefined || cy === undefined) {
                  return <svg key={`dot-${index}`} x={0} y={0} width={0} height={0} />;
                }
                return (
                  <circle
                    key={`dot-${index}`}
                    cx={cx}
                    cy={cy}
                    r={4}
                    fill="var(--danger)"
                    stroke="var(--surface-1)"
                    strokeWidth={1}
                  />
                );
              }}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {activeAlert && (
        <div className={`zone-card__alert zone-card__alert--${sev}`}>
          <strong>|z| = {Math.abs(activeAlert.z_score).toFixed(2)}</strong> — {activeAlert.reason}
          <div className="zone-card__alert-time">{new Date(activeAlert.timestamp).toLocaleTimeString()}</div>
        </div>
      )}
    </div>
  );
}
