import {
  Background, BackgroundVariant, BaseEdge, Controls, getBezierPath, Handle, MarkerType, Position, ReactFlow, type Edge, type EdgeProps, type Node, type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { motion, useReducedMotion } from "motion/react";
import { memo, useEffect, useMemo, useState } from "react";
import type { Condition, TaskStatus, TaskView } from "@/lib/api";
import { cx, ms } from "@/lib/format";

// ---------------------------------------------------------------- layout

/** Columns by topological depth, rows by order within a level. Used when a definition carries no positions. */
export function autoLayout(keys: string[], edges: { from: string; to: string }[]): Record<string, { x: number; y: number }> {
  const indeg: Record<string, number> = Object.fromEntries(keys.map((k) => [k, 0]));
  const out: Record<string, string[]> = Object.fromEntries(keys.map((k) => [k, []]));
  edges.forEach((e) => { if (e.to in indeg && e.from in out) { indeg[e.to]++; out[e.from].push(e.to); } });
  const level: Record<string, number> = {};
  let frontier = keys.filter((k) => indeg[k] === 0);
  let depth = 0;
  while (frontier.length) {
    const next: string[] = [];
    frontier.forEach((k) => { level[k] = depth; out[k].forEach((d) => { if (--indeg[d] === 0) next.push(d); }); });
    frontier = next;
    depth++;
  }
  const rows: Record<number, number> = {};
  const pos: Record<string, { x: number; y: number }> = {};
  keys.forEach((k) => { const l = level[k] ?? depth; const r = rows[l] ?? 0; rows[l] = r + 1; pos[k] = { x: l * 280, y: r * 130 }; });
  return pos;
}

export const conditionLabel = (c?: Condition | null) =>
  !c ? undefined : c.op.toLowerCase() === "exists" || c.op.toLowerCase() === "truthy"
    ? `${c.path} ${c.op.toLowerCase()}` : `${c.path} ${({ eq: "=", neq: "≠", gt: ">", gte: "≥", lt: "<", lte: "≤", in: "in" } as Record<string, string>)[c.op.toLowerCase()] ?? c.op} ${JSON.stringify(c.value)}`;

// ---------------------------------------------------------------- node

export const STATE_COLOR: Record<TaskStatus, string> = {
  PENDING: "var(--color-ink-3)", READY: "var(--color-ink-2)", QUEUED: "var(--color-queued)", RUNNING: "var(--color-cyan)",
  SUCCEEDED: "var(--color-ok)", FAILED: "var(--color-bad)", RETRYING: "var(--color-warn)", CANCELLED: "var(--color-ink-3)", SKIPPED: "var(--color-line)",
};
const STATE_LABEL: Record<TaskStatus, string> = {
  PENDING: "Pending", READY: "Ready", QUEUED: "Queued", RUNNING: "Running", SUCCEEDED: "Completed", FAILED: "Failed",
  RETRYING: "Retrying", CANCELLED: "Cancelled", SKIPPED: "Skipped",
};

export type TaskNodeData = {
  name: string; type: string; status?: TaskStatus; attempt?: number; maxAttempts?: number; startedAt?: string; durationMs?: number;
  worker?: string; selected?: boolean; invalid?: boolean;
};

function Elapsed({ since }: { since: string }) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const t = setInterval(() => setNow(Date.now()), 250); return () => clearInterval(t); }, []);
  return <>{ms(Math.max(0, now - new Date(since).getTime()))}</>;
}

export const TaskNode = memo(function TaskNode({ data }: NodeProps<Node<TaskNodeData>>) {
  const reduce = useReducedMotion();
  const status = data.status;
  const color = status ? STATE_COLOR[status] : "var(--color-violet)";
  const dim = status === "SKIPPED" || status === "CANCELLED";
  return (
    <motion.div
      initial={false}
      animate={{ borderColor: data.selected ? "var(--color-violet)" : status === "RUNNING" ? "var(--color-cyan)" : "var(--color-line)",
        opacity: dim ? 0.55 : 1 }}
      transition={{ duration: reduce ? 0 : 0.35 }}
      className={cx("relative w-[228px] rounded-[9px] border bg-surface text-left", status === "RUNNING" && !reduce && "fg-running-ring", data.invalid && "!border-bad")}
    >
      <Handle type="target" position={Position.Left} />
      {/* State rail: the node's colour is its state. */}
      <motion.div className="absolute inset-y-0 left-0 w-[3px] rounded-l-[9px]" initial={false} animate={{ backgroundColor: color }} transition={{ duration: reduce ? 0 : 0.35 }} />
      {status && !reduce && (
        <motion.div key={status} className="pointer-events-none absolute inset-0 rounded-lg"
          initial={{ boxShadow: `inset 0 0 0 1px ${color}`, opacity: 0.9 }} animate={{ opacity: 0 }} transition={{ duration: 1.1 }} />
      )}
      <div className="py-2.5 pl-4 pr-3">
        <div className="truncate text-[0.875rem] font-semibold leading-snug text-ink">{data.name}</div>
        <div className="truncate font-mono text-[0.68rem] text-ink-3">{data.type}</div>
        {status && (
          <div className="mt-2 flex items-center justify-between gap-2 border-t border-line-soft pt-2 text-[0.72rem]">
            <span className={cx("flex items-center gap-1.5 font-medium", status === "SKIPPED" && "text-ink-3")} style={{ color: status === "SKIPPED" ? undefined : color }}>
              <span className={cx("size-1.5 rounded-full", status === "RUNNING" && "fg-pulse")} style={{ background: color }} />
              {STATE_LABEL[status]}
            </span>
            <span className="tnum text-ink-3">
              {status === "RUNNING" && data.startedAt ? <Elapsed since={data.startedAt} /> : data.durationMs != null && ["SUCCEEDED", "FAILED"].includes(status) ? ms(data.durationMs) : null}
              {(data.attempt ?? 0) > 1 && <span className="ml-1.5 text-warn">attempt {data.attempt}</span>}
            </span>
          </div>
        )}
        {status === "RUNNING" && data.worker && <div className="mt-1 truncate font-mono text-[0.65rem] text-cyan/80">{data.worker}</div>}
      </div>
      <Handle type="source" position={Position.Right} />
    </motion.div>
  );
});

export const nodeTypes = { task: TaskNode };

/** Bezier edge; while its target executes, a particle rides it toward the running task. */
function FlowEdge(p: EdgeProps<Edge<{ live?: boolean }>>) {
  const reduce = useReducedMotion();
  const [path] = getBezierPath(p);
  return (
    <>
      <BaseEdge id={p.id} path={path} style={p.style} markerEnd={p.markerEnd} label={p.label} labelStyle={p.labelStyle}
        labelShowBg labelBgStyle={p.labelBgStyle} labelBgPadding={p.labelBgPadding} labelBgBorderRadius={4} labelX={(p.sourceX + p.targetX) / 2} labelY={(p.sourceY + p.targetY) / 2} />
      {p.data?.live && !reduce && (
        <circle r={3.2} fill="var(--color-cyan)" style={{ filter: "drop-shadow(0 0 4px var(--color-cyan))" }}>
          <animateMotion dur="1.1s" begin="0s" repeatCount="indefinite" path={path} />
        </circle>
      )}
    </>
  );
}
export const edgeTypes = { flow: FlowEdge };

// ---------------------------------------------------------------- live execution graph

export function LiveDag({ tasks, edges, selected, onSelect, height = 460 }: {
  tasks: TaskView[]; edges: { from: string; to: string; condition?: Condition | null }[];
  selected?: string | null; onSelect?: (key: string | null) => void; height?: number;
}) {
  const byKey = useMemo(() => Object.fromEntries(tasks.map((t) => [t.nodeKey, t])), [tasks]);
  const positioned = tasks.some((t) => t.x !== 0 || t.y !== 0);
  const layout = useMemo(() => (positioned ? null : autoLayout(tasks.map((t) => t.nodeKey), edges)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [positioned, tasks.length, edges.length]);

  const nodes: Node<TaskNodeData>[] = tasks.map((t) => ({
    id: t.nodeKey, type: "task", position: layout ? layout[t.nodeKey] : { x: t.x, y: t.y }, draggable: false,
    data: { name: t.name, type: t.taskType, status: t.status, attempt: t.attempt, maxAttempts: t.maxAttempts, startedAt: t.startedAt,
      durationMs: t.durationMs, worker: t.leaseOwner, selected: selected === t.nodeKey },
  }));

  const flowEdges: Edge[] = edges.map((e) => {
    const src = byKey[e.from]?.status, dst = byKey[e.to]?.status;
    const live = src === "SUCCEEDED" && (dst === "RUNNING" || dst === "QUEUED" || dst === "RETRYING");
    const done = src === "SUCCEEDED" && dst === "SUCCEEDED";
    const dead = dst === "SKIPPED" || dst === "CANCELLED" || src === "SKIPPED";
    const stroke = live ? "var(--color-cyan)" : done ? "color-mix(in oklab, var(--color-ok) 55%, transparent)" : dead ? "var(--color-line)" : "var(--color-ink-3)";
    return {
      id: `${e.from}->${e.to}`, source: e.from, target: e.to, type: "flow", data: { live }, className: live ? "fg-edge-live" : undefined,
      label: conditionLabel(e.condition), animated: false,
      style: { stroke, strokeWidth: live ? 2 : 1.4, strokeDasharray: dead ? "4 5" : undefined },
      labelStyle: { fill: "var(--color-ink-2)", fontSize: 11, fontFamily: "var(--font-mono)" },
      labelBgStyle: { fill: "var(--color-raised)" }, labelBgPadding: [4, 2] as [number, number],
      markerEnd: { type: MarkerType.ArrowClosed, color: stroke, width: 14, height: 14 },
    };
  });

  return (
    <div className="grid-paper overflow-hidden rounded-[10px] border border-line-soft" style={{ height }}>
      <ReactFlow nodes={nodes} edges={flowEdges} nodeTypes={nodeTypes} edgeTypes={edgeTypes} colorMode="dark" fitView fitViewOptions={{ padding: 0.12, maxZoom: 1.1 }}
        nodesConnectable={false} elementsSelectable proOptions={{ hideAttribution: true }} minZoom={0.3}
        onNodeClick={(_, n) => onSelect?.(n.id)} onPaneClick={() => onSelect?.(null)}>
        <Background variant={BackgroundVariant.Dots} gap={28} size={0} color="transparent" />
        <Controls showInteractive={false} position="bottom-right" />
      </ReactFlow>
    </div>
  );
}
