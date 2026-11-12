import { useEffect, useRef, useState } from "react";
import { session, type ExecEvent } from "./api";

export type LiveState = "connecting" | "live" | "reconnecting";

/**
 * Server-Sent Events over fetch (so the bearer token travels in a header, not the URL).
 * Reconnects with backoff; `onEvent` is kept in a ref so callers can pass inline closures.
 */
export function useEventStream(path: string | null, onEvent: (e: ExecEvent) => void): LiveState {
  const [state, setState] = useState<LiveState>("connecting");
  const handler = useRef(onEvent);
  handler.current = onEvent;

  useEffect(() => {
    if (!path) return;
    const ctrl = new AbortController();
    let attempt = 0;
    let stopped = false;

    async function run() {
      while (!stopped) {
        try {
          const res = await fetch(`/api/v1${path}`, {
            headers: { Authorization: `Bearer ${session.token()}`, Accept: "text/event-stream" },
            signal: ctrl.signal,
          });
          if (!res.ok || !res.body) throw new Error(`stream ${res.status}`);
          const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
          let buf = "";
          setState("live");
          attempt = 0;
          for (;;) {
            const { value, done } = await reader.read();
            if (done) break;
            buf += value;
            let idx;
            while ((idx = buf.indexOf("\n\n")) >= 0) {
              const frame = buf.slice(0, idx);
              buf = buf.slice(idx + 2);
              let name = "message";
              const data: string[] = [];
              for (const line of frame.split("\n")) {
                if (line.startsWith("event:")) name = line.slice(6).trim();
                else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
              }
              if (name === "execution-event" && data.length) {
                try { handler.current(JSON.parse(data.join("\n"))); } catch { /* ignore malformed */ }
              }
            }
          }
        } catch {
          if (stopped) return;
        }
        if (stopped) return;
        setState("reconnecting");
        attempt++;
        await new Promise((r) => setTimeout(r, Math.min(8000, 500 * 2 ** attempt)));
      }
    }
    run();
    return () => { stopped = true; ctrl.abort(); };
  }, [path]);

  return state;
}
