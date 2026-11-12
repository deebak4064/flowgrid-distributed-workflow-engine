import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  addEdge, Controls, MarkerType, ReactFlow, ReactFlowProvider, useEdgesState, useNodesState, useReactFlow,
  type Connection, type Edge, type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { GripVertical, Plus, Trash2 } from "lucide-react";
import { useCallback, useEffect, useMemo, useState, type DragEvent } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  api, session, type Condition, type DagAnalysis, type NodeDef, type TaskDefinition, type WorkflowDefinition, type WorkflowDetail,
} from "@/lib/api";
import { cx, ms, time } from "@/lib/format";
import { autoLayout, conditionLabel, nodeTypes, type TaskNodeData } from "@/components/Dag";
import { RunDialog } from "@/pages/Workflows";
import { Button, ErrorNote, Field, Input, Mono, Panel, Select, TabContent, TabList, Tabs, TabTrigger, Textarea } from "@/components/ui";

type BNodeData = TaskNodeData & { def: NodeDef };
type BNode = Node<BNodeData>;
type BEdge = Edge<{ when?: Condition | null }>;

const edgeStyle = (when?: Condition | null): Partial<BEdge> => ({
  label: conditionLabel(when), style: { stroke: when ? "var(--color-violet)" : "var(--color-ink-3)", strokeWidth: 1.5 },
  labelStyle: { fill: "var(--color-ink-2)", fontSize: 10, fontFamily: "var(--font-mono)" }, labelBgStyle: { fill: "var(--color-surface)" },
  markerEnd: { type: MarkerType.ArrowClosed, color: when ? "var(--color-violet)" : "var(--color-ink-3)" },
});

function toFlow(def: WorkflowDefinition): { nodes: BNode[]; edges: BEdge[] } {
  const hasPos = def.nodes.some((n) => n.position && (n.position.x !== 0 || n.position.y !== 0));
  const layout = hasPos ? null : autoLayout(def.nodes.map((n) => n.key), def.edges);
  return {
    nodes: def.nodes.map((n) => ({ id: n.key, type: "task", position: layout ? layout[n.key] : n.position!, data: { name: n.name, type: n.type, def: n } })),
    edges: def.edges.map((e) => ({ id: `${e.from}->${e.to}`, source: e.from, target: e.to, data: { when: e.when ?? null }, ...edgeStyle(e.when) })),
  };
}

function toDefinition(nodes: BNode[], edges: BEdge[]): WorkflowDefinition {
  return {
    nodes: nodes.map((n) => ({ ...n.data.def, key: n.id, name: n.data.name, position: { x: Math.round(n.position.x), y: Math.round(n.position.y) } })),
    edges: edges.map((e) => ({ from: e.source, to: e.target, when: e.data?.when ?? null })),
  };
}

/** Client-side Kahn pass for instant feedback; the server re-validates on save. Returns nodes on a cycle (unsorted remainder). */
function cycleNodes(keys: string[], edges: { source: string; target: string }[]): Set<string> {
  const indeg = new Map(keys.map((k) => [k, 0]));
  edges.forEach((e) => indeg.set(e.target, (indeg.get(e.target) ?? 0) + 1));
  const q = keys.filter((k) => indeg.get(k) === 0);
  const seen = new Set<string>();
  while (q.length) {
    const k = q.shift()!;
    seen.add(k);
    edges.filter((e) => e.source === k).forEach((e) => { indeg.set(e.target, indeg.get(e.target)! - 1); if (indeg.get(e.target) === 0) q.push(e.target); });
  }
  return new Set(keys.filter((k) => !seen.has(k)));
}

const slug = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 40) || "task";

function Palette({ defs, onAdd }: { defs: TaskDefinition[]; onAdd: (t: TaskDefinition) => void }) {
  const groups = useMemo(() => {
    const g: Record<string, TaskDefinition[]> = {};
    defs.forEach((d) => (g[d.category] ??= []).push(d));
    return Object.entries(g);
  }, [defs]);
  return (
    <div className="space-y-4">
      <p className="text-xs text-ink-3">Drag onto the canvas, or press a task type to add it.</p>
      {groups.map(([cat, items]) => (
        <div key={cat}>
          <div className="mb-1.5 text-xs font-medium text-ink-2">{cat}</div>
          <ul className="space-y-1">
            {items.map((d) => (
              <li key={d.type}>
                <button draggable onDragStart={(e) => { e.dataTransfer.setData("application/flowgrid-task", d.type); e.dataTransfer.effectAllowed = "move"; }}
                  onClick={() => onAdd(d)} title={d.description}
                  className="group flex w-full cursor-grab items-start gap-2 rounded-md border border-line-soft bg-ground/60 px-2 py-1.5 text-left hover:border-violet/60 active:cursor-grabbing">
                  <GripVertical size={13} className="mt-0.5 shrink-0 text-ink-3 group-hover:text-violet" aria-hidden />
                  <span className="min-w-0">
                    <span className="block truncate font-mono text-[0.7rem] text-ink">{d.type}</span>
                    <span className="block truncate text-2xs text-ink-3">{d.description}</span>
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
}

function NodeInspector({ node, onChange, onDelete, defaults }: { node: BNode; onChange: (n: BNode) => void; onDelete: () => void; defaults?: TaskDefinition }) {
  const d = node.data.def;
  const [params, setParams] = useState(JSON.stringify(d.params ?? {}, null, 2));
  const [paramErr, setParamErr] = useState(false);
  useEffect(() => { setParams(JSON.stringify(node.data.def.params ?? {}, null, 2)); setParamErr(false); }, [node.id]); // eslint-disable-line react-hooks/exhaustive-deps
  const set = (patch: Partial<NodeDef>) => onChange({ ...node, data: { ...node.data, def: { ...d, ...patch } } });
  const numOrNull = (v: string) => (v === "" ? null : Number(v));
  return (
    <div className="space-y-3">
      <Field label="Name"><Input value={node.data.name} onChange={(e) => onChange({ ...node, data: { ...node.data, name: e.target.value } })} /></Field>
      <Field label="Key" hint="Referenced by downstream tasks as results.<key>"><Input value={node.id} disabled className="font-mono text-xs" /></Field>
      <Field label="Task type"><Input value={d.type} disabled className="font-mono text-xs" /></Field>
      <Field label="Parameters (JSON)" hint={paramErr ? "Not valid JSON yet" : "Passed to the handler; also accepts a chaos object for failure injection"}>
        <Textarea rows={5} spellCheck={false} value={params} className={cx(paramErr && "!border-bad")}
          onChange={(e) => { setParams(e.target.value); try { set({ params: JSON.parse(e.target.value || "{}") }); setParamErr(false); } catch { setParamErr(true); } }} />
      </Field>
      <div className="grid grid-cols-2 gap-3">
        <Field label="Timeout (ms)" hint={defaults ? `default ${ms(defaults.defaultTimeoutMs)}` : undefined}>
          <Input type="number" min={100} value={d.timeoutMs ?? ""} onChange={(e) => set({ timeoutMs: numOrNull(e.target.value) })} />
        </Field>
        <Field label="Priority (0–10)"><Input type="number" min={0} max={10} value={d.priority ?? 5} onChange={(e) => set({ priority: numOrNull(e.target.value) })} /></Field>
        <Field label="Max attempts" hint={defaults ? `default ${defaults.defaultMaxAttempts}` : undefined}>
          <Input type="number" min={1} max={20} value={d.retry?.maxAttempts ?? ""} onChange={(e) => set({ retry: { ...d.retry, maxAttempts: numOrNull(e.target.value) } })} />
        </Field>
        <Field label="Backoff base (ms)" hint={defaults ? `default ${defaults.defaultBackoffMs}` : undefined}>
          <Input type="number" min={0} value={d.retry?.backoffMs ?? ""} onChange={(e) => set({ retry: { ...d.retry, backoffMs: numOrNull(e.target.value) } })} />
        </Field>
        <Field label="Deadline (ms after start)"><Input type="number" min={0} value={d.deadlineMs ?? ""} onChange={(e) => set({ deadlineMs: numOrNull(e.target.value) })} /></Field>
        <Field label="Runs when">
          <Select className="w-full" value={d.trigger ?? "ALL_SUCCESS"} onChange={(e) => set({ trigger: e.target.value as NodeDef["trigger"] })}>
            <option value="ALL_SUCCESS">All upstream succeed</option><option value="ANY_SUCCESS">Any upstream succeeds</option>
          </Select>
        </Field>
      </div>
      <Button variant="danger" size="sm" onClick={onDelete}><Trash2 size={13} />Remove task</Button>
    </div>
  );
}

function EdgeInspector({ edge, onChange, onDelete }: { edge: BEdge; onChange: (w: Condition | null) => void; onDelete: () => void }) {
  const w = edge.data?.when;
  const [value, setValue] = useState(w?.value === undefined ? "" : JSON.stringify(w.value));
  useEffect(() => setValue(edge.data?.when?.value === undefined ? "" : JSON.stringify(edge.data.when.value)), [edge.id]); // eslint-disable-line react-hooks/exhaustive-deps
  const parse = (v: string) => { try { return JSON.parse(v); } catch { return v; } };
  return (
    <div className="space-y-3 text-sm">
      <p className="text-ink-2"><Mono>{edge.source}</Mono> runs before <Mono>{edge.target}</Mono>.</p>
      <label className="flex items-center gap-2 text-sm">
        <input type="checkbox" checked={!!w} onChange={(e) => onChange(e.target.checked ? { path: "route", op: "eq", value: "" } : null)} className="accent-violet" />
        Only when the upstream output matches a condition
      </label>
      {w && (
        <div className="grid grid-cols-[1fr_88px] gap-2">
          <Field label="Output path"><Input value={w.path} onChange={(e) => onChange({ ...w, path: e.target.value })} className="font-mono text-xs" /></Field>
          <Field label="Operator">
            <Select className="w-full" value={w.op.toLowerCase()} onChange={(e) => onChange({ ...w, op: e.target.value })}>
              {["eq", "neq", "gt", "gte", "lt", "lte", "in", "exists", "truthy"].map((o) => <option key={o}>{o}</option>)}
            </Select>
          </Field>
          {!["exists", "truthy"].includes(w.op.toLowerCase()) && (
            <div className="col-span-2"><Field label="Value (JSON or text)"><Input value={value} className="font-mono text-xs"
              onChange={(e) => { setValue(e.target.value); onChange({ ...w, value: parse(e.target.value) }); }} /></Field></div>
          )}
        </div>
      )}
      <p className="text-xs text-ink-3">When the condition is false the downstream task is skipped, which lets one workflow branch on results.</p>
      <Button variant="danger" size="sm" onClick={onDelete}><Trash2 size={13} />Remove dependency</Button>
    </div>
  );
}

function DependencyEditor({ nodes, edges, setEdges, cycle, analysis }: {
  nodes: BNode[]; edges: BEdge[]; setEdges: (f: (e: BEdge[]) => BEdge[]) => void; cycle: Set<string>; analysis: DagAnalysis | null;
}) {
  const toggle = (from: string, to: string) => setEdges((es) => es.some((e) => e.source === from && e.target === to)
    ? es.filter((e) => !(e.source === from && e.target === to))
    : [...es, { id: `${from}->${to}`, source: from, target: to, data: { when: null }, ...edgeStyle(null) }]);
  return (
    <div className="grid gap-5 xl:grid-cols-[1fr_320px]">
      <Panel title="Dependency matrix" bodyClass="overflow-x-auto p-0">
        <table className="text-xs">
          <thead>
            <tr>
              <th className="sticky left-0 bg-surface px-3 py-2 text-left font-medium text-ink-3">Task, depends on column</th>
              {nodes.map((n) => <th key={n.id} className="px-1 py-2 font-mono font-normal text-ink-3"><span className="inline-block max-w-20 truncate" title={n.id}>{n.id}</span></th>)}
            </tr>
          </thead>
          <tbody>
            {nodes.map((row) => (
              <tr key={row.id} className="border-t border-line-soft">
                <td className={cx("sticky left-0 bg-surface px-3 py-1.5", cycle.has(row.id) && "text-bad")}>{row.data.name} <Mono className="ml-1">{row.id}</Mono></td>
                {nodes.map((col) => {
                  const on = edges.some((e) => e.source === col.id && e.target === row.id);
                  const cond = edges.find((e) => e.source === col.id && e.target === row.id)?.data?.when;
                  return (
                    <td key={col.id} className="px-1 py-1 text-center">
                      {col.id === row.id ? <span className="text-ink-3">·</span> : (
                        <button aria-label={`${row.id} depends on ${col.id}`} aria-pressed={on} onClick={() => toggle(col.id, row.id)}
                          title={cond ? conditionLabel(cond) : undefined}
                          className={cx("size-6 rounded border", on ? cond ? "border-violet bg-violet/25" : "border-cyan/70 bg-cyan/20" : "border-line-soft hover:border-ink-3")} />
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
      <Panel title="Execution order">
        {cycle.size > 0 ? (
          <p className="text-sm text-bad">These tasks form a cycle and can never start: {[...cycle].join(", ")}. Remove one of their dependencies.</p>
        ) : analysis?.levels?.length ? (
          <ol className="space-y-2 text-sm">
            {analysis.levels.map((lvl, i) => (
              <li key={i} className="flex gap-3">
                <span className="tnum w-14 shrink-0 text-xs text-ink-3">Stage {i + 1}</span>
                <span className="flex flex-wrap gap-1">{lvl.map((k) => <Mono key={k} className="rounded bg-raised px-1.5 py-0.5 text-ink">{k}</Mono>)}</span>
              </li>
            ))}
            <li className="pt-2 text-xs text-ink-3">Tasks in the same stage can run in parallel.</li>
          </ol>
        ) : <p className="text-sm text-ink-3">Validate to see the topological stages.</p>}
      </Panel>
    </div>
  );
}

function BuilderInner({ tab }: { tab?: string }) {
  const { id } = useParams();
  const isNew = !id;
  const nav = useNavigate();
  const qc = useQueryClient();
  const flow = useReactFlow();
  const isAdmin = session.user()?.role === "ADMIN";
  const [nodes, setNodes, onNodesChange] = useNodesState<BNode>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<BEdge>([]);
  const [sel, setSel] = useState<{ kind: "node" | "edge"; id: string } | null>(null);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [note, setNote] = useState("");
  const [analysis, setAnalysis] = useState<DagAnalysis | null>(null);
  const [err, setErr] = useState<unknown>(null);
  const [saved, setSaved] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [viewVersion, setViewVersion] = useState<number | null>(null);

  const defs = useQuery({ queryKey: ["task-definitions"], queryFn: () => api<TaskDefinition[]>("/task-definitions") });
  const wf = useQuery({ queryKey: ["workflow", id], queryFn: () => api<WorkflowDetail>(`/workflows/${id}`), enabled: !isNew });
  const versionDef = useQuery({ queryKey: ["workflow", id, "v", viewVersion], enabled: viewVersion !== null,
    queryFn: () => api<WorkflowDefinition>(`/workflows/${id}/versions/${viewVersion}`) });

  useEffect(() => {
    if (!wf.data) return;
    const f = toFlow(wf.data.definition);
    setNodes(f.nodes); setEdges(f.edges); setName(wf.data.name); setDescription(wf.data.description);
    setAnalysis(wf.data.analysis ?? null);
  }, [wf.data, setNodes, setEdges]);

  const cycle = useMemo(() => cycleNodes(nodes.map((n) => n.id), edges), [nodes, edges]);
  const displayNodes = useMemo(() => nodes.map((n) => ({ ...n, data: { ...n.data, invalid: cycle.has(n.id), selected: sel?.kind === "node" && sel.id === n.id } })), [nodes, cycle, sel]);

  const addTask = useCallback((t: TaskDefinition, position?: { x: number; y: number }) => {
    const base = slug(t.type.split(".").pop() ?? "task");
    let key = base, i = 2;
    const keys = new Set(nodes.map((n) => n.id));
    while (keys.has(key)) key = `${base}_${i++}`;
    const pos = position ?? flow.screenToFlowPosition({ x: window.innerWidth / 2, y: window.innerHeight / 2 });
    const name = t.description.split(/[;:(]/)[0].slice(0, 40);
    setNodes((ns) => [...ns, { id: key, type: "task", position: pos, data: { name, type: t.type, def: { key, name, type: t.type, params: {}, priority: 5 } } }]);
    setSel({ kind: "node", id: key });
  }, [nodes, flow, setNodes]);

  const onDrop = (e: DragEvent) => {
    e.preventDefault();
    const type = e.dataTransfer.getData("application/flowgrid-task");
    const t = defs.data?.find((d) => d.type === type);
    if (t) addTask(t, flow.screenToFlowPosition({ x: e.clientX - 100, y: e.clientY - 30 }));
  };

  const onConnect = useCallback((c: Connection) => {
    if (c.source === c.target) return;
    setEdges((es) => addEdge({ ...c, id: `${c.source}->${c.target}`, data: { when: null }, ...edgeStyle(null) }, es));
  }, [setEdges]);

  async function validate() {
    setErr(null);
    try { setAnalysis(await api<DagAnalysis>("/workflows/validate", { method: "POST", json: toDefinition(nodes, edges) })); }
    catch (e) { setErr(e); }
  }

  async function save() {
    setErr(null); setSaved(null);
    const definition = toDefinition(nodes, edges);
    try {
      if (isNew) {
        const r = await api<WorkflowDetail>("/workflows", { method: "POST", json: { name, description, tags: [], definition } });
        qc.invalidateQueries({ queryKey: ["workflows"] });
        nav(`/workflows/${r.id}`, { replace: true });
      } else {
        const r = await api<WorkflowDetail>(`/workflows/${id}`, { method: "PUT", json: { expectedLockVersion: wf.data?.lockVersion, description, definition, changeNote: note } });
        qc.setQueryData(["workflow", id], r);
        setSaved(r.currentVersion === wf.data?.currentVersion ? "No changes to publish." : `Published version ${r.currentVersion}.`);
        setNote("");
      }
    } catch (e) { setErr(e); }
  }

  const selNode = sel?.kind === "node" ? nodes.find((n) => n.id === sel.id) : undefined;
  const selEdge = sel?.kind === "edge" ? edges.find((e) => e.id === sel.id) : undefined;

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div className="min-w-0">
          <div className="text-xs text-ink-3"><Link to="/workflows" className="hover:text-ink">Workflows</Link> / {isNew ? "New" : name}</div>
          {isNew ? (
            <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="workflow-name" className="mt-1 h-9 w-80 text-base font-semibold" aria-label="Workflow name" />
          ) : (
            <h1 className="mt-1 text-[1.75rem] font-semibold leading-tight tracking-[-0.02em]">{name} <span className="text-sm font-normal text-ink-3">v{wf.data?.currentVersion}</span></h1>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          <Button onClick={validate}>Validate</Button>
          {!isNew && session.user()?.role !== "VIEWER" && <Button onClick={() => setRunning(true)}>Run</Button>}
          {isAdmin && (
            <>
              {!isNew && <Input value={note} onChange={(e) => setNote(e.target.value)} placeholder="Change note" className="w-48" aria-label="Change note" />}
              <Button variant="primary" onClick={save} disabled={cycle.size > 0 || nodes.length === 0 || (isNew && !name)}>{isNew ? "Create workflow" : "Publish new version"}</Button>
            </>
          )}
        </div>
      </div>
      {wf.data && <RunDialog workflow={wf.data} open={running} onOpenChange={setRunning} />}
      <ErrorNote error={err} />
      {saved && <p className="text-sm text-ok">{saved}</p>}
      {analysis && analysis.errors.length > 0 && <ErrorNote error={{ message: "The definition has problems", errors: analysis.errors }} />}
      {analysis && analysis.valid && analysis.errors.length === 0 && <p className="text-sm text-ok">Valid DAG: {analysis.topologicalOrder.length} tasks in {analysis.levels.length} stages.</p>}

      <Tabs value={tab === "dependencies" ? "dependencies" : tab === "versions" ? "versions" : "canvas"}
        onValueChange={(v) => nav(isNew ? "/workflows/new" : v === "canvas" ? `/workflows/${id}` : v === "dependencies" ? `/workflows/${id}/dependencies` : `/workflows/${id}?versions`, { replace: true })}>
        <TabList>
          <TabTrigger value="canvas">Canvas</TabTrigger>
          {!isNew && <TabTrigger value="dependencies">Dependencies</TabTrigger>}
        </TabList>
        <TabContent value="canvas">
          <div className="grid gap-4 xl:grid-cols-[230px_1fr_300px]">
            <Panel title="Task types" className="xl:max-h-[620px] xl:overflow-y-auto">
              {defs.data ? <Palette defs={defs.data} onAdd={(t) => addTask(t)} /> : <p className="text-xs text-ink-3">Loading…</p>}
            </Panel>
            <div className="grid-paper relative h-[620px] overflow-hidden rounded-lg border border-line-soft" onDrop={onDrop} onDragOver={(e) => { e.preventDefault(); e.dataTransfer.dropEffect = "move"; }}>
              <ReactFlow nodes={displayNodes} edges={edges} nodeTypes={nodeTypes} colorMode="dark" onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
                onConnect={onConnect} fitView fitViewOptions={{ padding: 0.06, maxZoom: 1.1 }} deleteKeyCode={isAdmin ? ["Backspace", "Delete"] : null}
                nodesDraggable={isAdmin} nodesConnectable={isAdmin} proOptions={{ hideAttribution: true }}
                onNodeClick={(_, n) => setSel({ kind: "node", id: n.id })} onEdgeClick={(_, e) => setSel({ kind: "edge", id: e.id })} onPaneClick={() => setSel(null)}>
                <Controls position="bottom-right" showInteractive={false} />
              </ReactFlow>
              {nodes.length === 0 && (
                <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
                  <div className="text-center text-sm text-ink-2"><Plus className="mx-auto mb-2 text-ink-3" />Drag a task type here to start the graph.<br />
                    <span className="text-xs text-ink-3">Connect a task's right handle to another's left handle to add a dependency.</span></div>
                </div>
              )}
            </div>
            <Panel title={selNode ? "Task" : selEdge ? "Dependency" : "Inspector"} className="xl:max-h-[620px] xl:overflow-y-auto">
              {selNode ? (
                <NodeInspector node={selNode} defaults={defs.data?.find((d) => d.type === selNode.data.type)}
                  onChange={(n) => setNodes((ns) => ns.map((x) => (x.id === n.id ? n : x)))}
                  onDelete={() => { setNodes((ns) => ns.filter((x) => x.id !== selNode.id)); setEdges((es) => es.filter((e) => e.source !== selNode.id && e.target !== selNode.id)); setSel(null); }} />
              ) : selEdge ? (
                <EdgeInspector edge={selEdge}
                  onChange={(when) => setEdges((es) => es.map((e) => (e.id === selEdge.id ? { ...e, data: { when }, ...edgeStyle(when) } : e)))}
                  onDelete={() => { setEdges((es) => es.filter((e) => e.id !== selEdge.id)); setSel(null); }} />
              ) : (
                <div className="space-y-3 text-sm text-ink-2">
                  <p>Select a task to edit its parameters, timeout, retry policy and priority, or a dependency to add a branch condition.</p>
                  <Field label="Description"><Textarea rows={3} value={description} onChange={(e) => setDescription(e.target.value)} className="font-sans text-sm" /></Field>
                  {!isNew && wf.data && (
                    <div>
                      <div className="mb-1 text-xs text-ink-3">Versions</div>
                      <ul className="space-y-1">
                        {wf.data.versions.map((v) => (
                          <li key={v.version}>
                            <button onClick={() => setViewVersion(viewVersion === v.version ? null : v.version)}
                              className={cx("flex w-full justify-between rounded px-2 py-1 text-left text-xs hover:bg-raised", viewVersion === v.version && "bg-raised")}>
                              <span><b className="text-ink">v{v.version}</b> {v.changeNote}</span><span className="text-ink-3">{time(v.createdAt, true)}</span>
                            </button>
                          </li>
                        ))}
                      </ul>
                      {viewVersion !== null && versionDef.data && (
                        <p className="mt-2 text-xs text-ink-3">v{viewVersion}: {versionDef.data.nodes.length} tasks, {versionDef.data.edges.length} dependencies.{" "}
                          {isAdmin && <button className="text-violet hover:underline" onClick={() => { const f = toFlow(versionDef.data!); setNodes(f.nodes); setEdges(f.edges); }}>Load into editor</button>}
                        </p>
                      )}
                    </div>
                  )}
                </div>
              )}
            </Panel>
          </div>
        </TabContent>
        <TabContent value="dependencies">
          <DependencyEditor nodes={nodes} edges={edges} setEdges={setEdges} cycle={cycle} analysis={analysis} />
        </TabContent>
      </Tabs>
    </div>
  );
}

export default function Builder({ tab }: { tab?: string }) {
  return <ReactFlowProvider><BuilderInner tab={tab} /></ReactFlowProvider>;
}
