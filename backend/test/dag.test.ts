// Unit tests for the pure logic: DAG validation, cycle detection, dependency resolution, conditions, backoff.
import { describe, expect, it } from "vitest";
import { backoffMs, evaluate, validateDag } from "../src/dag.ts";

const n = (key: string, extra = {}) => ({ key, name: key, type: "core.noop", ...extra });

describe("DAG cycle detection", () => {
  it("rejects a simple cycle and reports the concrete path", () => {
    const r = validateDag({ nodes: [n("root"), n("a"), n("b"), n("c")], edges: [{ from: "root", to: "a" }, { from: "a", to: "b" }, { from: "b", to: "c" }, { from: "c", to: "a" }] });
    expect(r.valid).toBe(false);
    expect(r.errors.join()).toMatch(/cyclic dependency detected: a -> b -> c -> a/);
  });
  it("rejects self-dependencies and graphs without a root", () => {
    expect(validateDag({ nodes: [n("a")], edges: [{ from: "a", to: "a" }] }).errors).toContain("node 'a' depends on itself");
    const r = validateDag({ nodes: [n("a"), n("b")], edges: [{ from: "a", to: "b" }, { from: "b", to: "a" }] });
    expect(r.errors).toContain("workflow has no root task (every task has a dependency)");
  });
  it("rejects unknown nodes, duplicate keys/edges and unknown task types", () => {
    const r = validateDag({ nodes: [n("a"), n("a"), n("b", { type: "nope" })], edges: [{ from: "a", to: "zz" }, { from: "a", to: "b" }, { from: "a", to: "b" }] }, new Set(["core.noop"]));
    expect(r.errors).toEqual(expect.arrayContaining(["duplicate node key 'a'", "node 'b': unknown task type 'nope'", "edge references unknown node 'zz'", "duplicate edge a -> b"]));
  });
  it("validates policy bounds", () => {
    const r = validateDag({ nodes: [n("a", { timeoutMs: 5, priority: 11, retry: { maxAttempts: 0 } })] });
    expect(r.errors).toEqual(["node 'a': timeoutMs must be between 100 and 3600000", "node 'a': priority must be between 0 and 10",
      "node 'a': retry.maxAttempts must be between 1 and 20"]);
    expect(validateDag({ nodes: [n("b", { trigger: "ANY_SUCCESS" })] }).errors).toEqual(["node 'b': ANY_SUCCESS requires at least one dependency"]);
  });
});

describe("dependency resolution / topological scheduling", () => {
  it("orders a diamond and groups independent tasks into parallel levels", () => {
    const r = validateDag({ nodes: [n("join"), n("left"), n("start"), n("right")],
      edges: [{ from: "start", to: "left" }, { from: "start", to: "right" }, { from: "left", to: "join" }, { from: "right", to: "join" }] });
    expect(r.valid).toBe(true);
    expect(r.levels).toEqual([["start"], ["left", "right"], ["join"]]);
    const pos = (k: string) => r.topologicalOrder.indexOf(k);
    expect(pos("start")).toBeLessThan(pos("left"));
    expect(pos("right")).toBeLessThan(pos("join"));
  });
  it("handles a 200-node chain in linear time", () => {
    const nodes = Array.from({ length: 200 }, (_, i) => n(`t${i}`));
    const edges = nodes.slice(1).map((x, i) => ({ from: `t${i}`, to: x.key }));
    const r = validateDag({ nodes, edges });
    expect(r.valid).toBe(true);
    expect(r.levels).toHaveLength(200);
  });
});

describe("edge conditions (branching)", () => {
  const out = { route: "AUTO_APPROVE", score: 42, tags: ["a"], nested: { ok: true } };
  it.each([
    [{ path: "route", op: "EQ", value: "AUTO_APPROVE" }, true], [{ path: "route", op: "neq", value: "REJECT" }, true],
    [{ path: "score", op: "gt", value: 40 }, true], [{ path: "score", op: "lte", value: 41 }, false], [{ path: "score", op: "gt", value: "40" }, false],
    [{ path: "route", op: "in", value: ["X", "AUTO_APPROVE"] }, true], [{ path: "nested.ok", op: "truthy" }, true],
    [{ path: "missing", op: "exists" }, false], [{ path: "route", op: "bogus" }, false],
  ])("%o -> %s", (cond, expected) => expect(evaluate(cond, out)).toBe(expected));
  it("treats a missing condition as always taken", () => expect(evaluate(null, {})).toBe(true));
});

describe("retry backoff", () => {
  it("is exponential with equal jitter and capped", () => {
    for (let attempt = 1; attempt <= 12; attempt++) {
      const cap = Math.min(10_000, 200 * 2 ** (attempt - 1));
      for (const r of [0, 0.5, 0.999999]) {
        const d = backoffMs(attempt, 200, 10_000, () => r);
        expect(d).toBeGreaterThanOrEqual(Math.floor(cap / 2));
        expect(d).toBeLessThanOrEqual(cap);
      }
    }
    expect(backoffMs(3, 0, 1000)).toBe(0);
  });
});
