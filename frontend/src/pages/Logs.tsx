import { useInfiniteQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router-dom";
import { api, qs, type ExecEvent } from "@/lib/api";
import { cx, shortId, time } from "@/lib/format";
import { Button, Empty, ErrorNote, Input, Mono, PageHeader, Panel, Select } from "@/components/ui";

const TYPES = ["", "WORKFLOW_", "TASK_STARTED", "TASK_SUCCEEDED", "TASK_RETRYING", "TASK_FAILED", "TASK_DEAD_LETTERED", "TASK_CANCELLED", "TASK_SKIPPED", "TASK_QUEUED"];
const TONE: Record<string, string> = { TASK_FAILED: "text-bad", WORKFLOW_FAILED: "text-bad", TASK_DEAD_LETTERED: "text-bad", TASK_RETRYING: "text-warn",
  TASK_SUCCEEDED: "text-ok", WORKFLOW_SUCCEEDED: "text-ok", TASK_STARTED: "text-cyan" };

export default function Logs() {
  const [type, setType] = useState("");
  const [draft, setDraft] = useState("");
  const [q, setQ] = useState("");
  const [executionId, setExecutionId] = useState("");
  const list = useInfiniteQuery({
    queryKey: ["logs", type, q, executionId], initialPageParam: undefined as number | undefined,
    queryFn: ({ pageParam }) => api<{ items: (ExecEvent & { id: number })[]; nextCursor?: number }>(
      `/events${qs({ type, q, executionId: /^[0-9a-f-]{36}$/.test(executionId) ? executionId : undefined, before: pageParam, size: 100 })}`),
    getNextPageParam: (p) => p.nextCursor ?? undefined,
  });
  const rows = list.data?.pages.flatMap((p) => p.items) ?? [];
  return (
    <div>
      <PageHeader title="Logs" description="Structured execution events from every scheduler and worker, newest first. Each process also writes structured JSON logs to stdout." />
      <Panel bodyClass="p-0" title={
        <form className="flex flex-wrap gap-2" onSubmit={(e) => { e.preventDefault(); setQ(draft); }}>
          <Select value={type} onChange={(e) => setType(e.target.value)} aria-label="Event type">
            {TYPES.map((t) => <option key={t} value={t}>{t ? t.toLowerCase().replace(/_$/, " events") : "All event types"}</option>)}
          </Select>
          <Input value={draft} onChange={(e) => setDraft(e.target.value)} placeholder="Search message, task or worker" className="w-64" aria-label="Search" />
          <Input value={executionId} onChange={(e) => setExecutionId(e.target.value.trim())} placeholder="Full execution id" className="w-72 font-mono text-xs" aria-label="Execution id" />
          <Button type="submit">Search</Button>
        </form>}>
        <ErrorNote error={list.error} />
        {rows.length === 0 && !list.isLoading ? <Empty title="No events match" /> : (
          <div className="overflow-x-auto">
            <table className="w-full font-mono text-[0.72rem]">
              <thead><tr className="text-left text-ink-3 [&_th]:px-3 [&_th]:py-2 [&_th]:font-sans [&_th]:text-xs [&_th]:font-medium">
                <th>Time</th><th>Event</th><th>Execution</th><th>Task</th><th>Attempt</th><th>Worker</th><th>Message</th></tr></thead>
              <tbody>
                {rows.map((e) => (
                  <tr key={e.id} className="border-t border-line-soft hover:bg-raised/40 [&_td]:px-3 [&_td]:py-1.5">
                    <td className="whitespace-nowrap text-ink-3">{time(e.at, true)}</td>
                    <td className={cx("whitespace-nowrap", TONE[e.type] ?? "text-ink-2")}>{e.type.toLowerCase()}</td>
                    <td><Link to={`/executions/${e.executionId}`} className="text-ink-2 hover:text-violet">{shortId(e.executionId)}</Link></td>
                    <td className="text-ink">{e.nodeKey ?? ""}</td>
                    <td className="text-ink-2">{e.attempt ?? ""}</td>
                    <td className="max-w-40 truncate text-ink-2">{e.workerId ?? ""}</td>
                    <td className="max-w-xl truncate text-ink-2" title={e.message}>{e.message ?? (e.from && e.to ? `${e.from.toLowerCase()} to ${e.to.toLowerCase()}` : "")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="flex justify-center p-3">
          {list.hasNextPage && <Button size="sm" onClick={() => list.fetchNextPage()} disabled={list.isFetchingNextPage}>{list.isFetchingNextPage ? "Loading…" : "Load older events"}</Button>}
          {!list.hasNextPage && rows.length > 0 && <Mono className="text-ink-3">End of retained history</Mono>}
        </div>
      </Panel>
    </div>
  );
}
