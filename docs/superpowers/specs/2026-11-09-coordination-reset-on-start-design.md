# Coordination reset during deployment

The deployment environment is unknown. Provide the PostgreSQL-only environment
toggle `LLMGW_COORDINATION_RESET_ON_START=true`, default false, in the gateway
itself. Operators drain requests and stop all old gateway processes first, enable
the toggle for the new deployment, and disable it afterwards.

## Approaches

- A deployment script cannot stop replicas in an unknown orchestrator.
- A separate reset command requires an extra deployment step; the user rejected it.
- The requested startup toggle performs the reset inside registration, then starts
  normally. It does not permanently bypass compatibility checks.

## Behavior

On initial registration only, delete rows in `coordination_replicas` whose policy
fingerprint differs from the starting process. Preserve matching registrations,
including new replicas that have already started. Delete and register in one
transaction under an exclusive writer lock on that table; concurrent matching
starts must succeed without erasing each other. A failed transaction must leave
the original registrations intact. Heartbeats/recovery never perform resets.

Do not clear leases, rate state, circuits, operation receipts, configuration, or
analytics. The toggle is not part of the policy fingerprint. Emit a warning when
enabled. Reject malformed boolean values and enabling the toggle for SQLite;
explicit false remains valid for the default SQLite Compose profile.

This opt-in assumes old processes are stopped and the new replicas have identical
coordination settings. It cannot stop remote processes or make mixed-version
rolling upgrades compatible. Disable it after deployment so future restarts
restore the ordinary startup guard. No schema migration is required.

## Verification

Exercise configuration validation and real PostgreSQL application startup: refusal
without reset, reset with preservation of matching peers, concurrent starts,
rollback on failed registration, and normal incompatibility rejection on later
heartbeat. Run the default suite and relevant PostgreSQL/race tests.
