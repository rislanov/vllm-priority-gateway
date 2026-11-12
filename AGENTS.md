<!-- codebase-memory-mcp:start -->
# Codebase Memory

## Codebase Knowledge Graph (codebase-memory-mcp)

This project uses codebase-memory-mcp to maintain a knowledge graph of the codebase.
ALWAYS prefer MCP graph tools over grep/glob/file-search for code discovery.

### Priority Order
1. `search_graph` — find functions, classes, routes, variables by pattern
2. `trace_path` — trace who calls a function or what it calls
3. `get_code_snippet` — read specific function/class source code
4. `check_index_coverage` — validate candidate paths and missed ranges before claims
5. `query_graph` — run Cypher queries for complex patterns
6. `get_architecture` — high-level project summary

### Evidence tiers
- **Scout (Tier 1):** quick positive lookup with few calls and targeted source checks. Mark it provisional; do not make negative or exhaustive claims.
- **Verify (Tier 2, default):** task-directed graph evidence, relevant trace directions, exact snippets for material claims, and relevant pagination.
- **Auditor (Tier 3):** bounded-scope full verification with current generation, complete relevant pagination, both call directions and broader relationships when material, and every limitation disclosed.
- After candidate paths are known in any tier, call `check_index_coverage` once with every evidence path. Add relevant scopes for negative or exhaustive claims. A clean result means no recorded gap, not proof of completeness. For partial, skipped, excluded, stale, pending, or unknown coverage, read/grep the reported ranges or scope before relying on graph results.

### When to fall back to grep/glob
- Searching for string literals, error messages, config values
- Searching non-code files (Dockerfiles, shell scripts, configs)
- When MCP tools return insufficient results

### Examples
- Find a handler: `search_graph(name_pattern=".*OrderHandler.*")`
- Who calls it: `trace_path(function_name="OrderHandler", direction="inbound")`
- Read source: `get_code_snippet(qualified_name="pkg/orders.OrderHandler")`

### Session resets and subagents
- At session start or after compaction, confirm the nearest graph project and generation with `list_projects` or `index_status`, then choose Scout, Verify, or Auditor.
- Before spawning a subagent, query the graph and coverage in the parent. Pass the tier, project, generation/freshness, bounded scope, queries and pagination state, qualified symbols, paths, call-chain findings, coverage evidence with ranges/reasons, source fallback already performed, and unresolved questions in the delegated task context.
- Do not assume subagents inherit MCP access or the parent conversation. If a child lacks MCP tools, it must not call or claim MCP access. It should use the supplied evidence and read/grep exact source, especially every reported missed-coverage range.
<!-- codebase-memory-mcp:end -->

# Coordination compatibility

Follow [the coordination compatibility policy](docs/coordination-compatibility.md).

- v0.7.0 is the baseline: coordination contract 4, rate algorithm 1. Preserve
  mixed-version compatibility from v0.7.0 through the remaining 0.x releases.
  Apply the same rule within each subsequent major. This is an explicit project
  commitment even before 1.0; it does not cover earlier 0.x contracts.
- Do not bump the coordination contract for an ordinary application release.
  Incompatible changes belong to a new application major with an explicit
  transition procedure; keeping the version constant does not make a change safe.
- Preserve the exact policy fingerprint for identical effective settings,
  including defaults and the JSON shape/order of ReplicaPolicy and nested types.
  Do not add unrelated feature settings to that serialized structure.
- Preserve shared lease, rate, reservation, circuit and replay semantics for
  old and new processes, including renewal, completion, recovery and cleanup.
  Never bypass compatibility checks or reset live registrations to enable rollout.
- Schema changes must preserve old-process operation AND restart/rollback.
  Store.Open currently runs migrations on every start: a newer migration version
  can prevent an old binary from starting even when all SQL changes are additive.
  Resolve and test that boundary before introducing a migration in this series.
- New functionality must work safely during mixed-version operation or remain
  disabled until all serving replicas support it. Do not promise nonblocking
  token-policy activation: current legacy leases can cause token_policy_transition.
- Document and verify affected old/new binary combinations for coordination or
  schema changes. Application version, coordination settings, database compatibility
  and graceful stream draining are separate rollout requirements.
