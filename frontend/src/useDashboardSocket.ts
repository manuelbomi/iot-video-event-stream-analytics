import { useEffect, useRef, useState } from "react";
import type { DashboardSnapshot } from "./types";

export type ConnectionStatus = "connecting" | "live" | "disconnected";

/**
 * Connects to the dashboard backend's `WS /ws` endpoint (see
 * `src/dashboard/api.py`) and keeps the most recently received snapshot in
 * state. The backend pushes a full `DashboardSnapshot` immediately on
 * connect and roughly once a second afterwards -- there is no incremental
 * message format, so we simply replace state on every message.
 *
 * Reconnects with a fixed backoff on disconnect, mirroring the retry logic
 * in the backend's own bundled `GET /` HTML page.
 */
export function useDashboardSocket(path = "/ws"): {
  snapshot: DashboardSnapshot | null;
  status: ConnectionStatus;
} {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    let cancelled = false;
    let socket: WebSocket | null = null;

    const connect = () => {
      if (cancelled) return;
      const proto = window.location.protocol === "https:" ? "wss" : "ws";
      socket = new WebSocket(`${proto}://${window.location.host}${path}`);

      socket.onopen = () => setStatus("live");

      socket.onmessage = (event: MessageEvent<string>) => {
        try {
          const data: DashboardSnapshot = JSON.parse(event.data);
          setSnapshot(data);
        } catch {
          // Ignore malformed frames rather than crashing the dashboard.
        }
      };

      socket.onclose = () => {
        if (cancelled) return;
        setStatus("disconnected");
        retryTimer.current = setTimeout(connect, 2000);
      };

      socket.onerror = () => {
        socket?.close();
      };
    };

    connect();

    return () => {
      cancelled = true;
      if (retryTimer.current) clearTimeout(retryTimer.current);
      socket?.close();
    };
  }, [path]);

  return { snapshot, status };
}
