# Coordination compatibility policy

## Decision and scope

Starting with **v0.7.0**, preserve the PostgreSQL coordination protocol within
each application major version. The baseline is coordination contract **4** and
rate algorithm **1**. All subsequent 0.x releases must remain compatible with
v0.7.0 and one another for mixed-version operation with identical effective
coordination settings. This is an explicit project rule despite the pre-1.0
version number. Earlier 0.x releases used different contracts and are excluded.

Keep releasing application changes normally (0.7.1, 0.8.0, and so on); do not
reuse the 0.7.0 version for different binaries. The coordination contract stays
fixed independently of the application version. A genuinely incompatible change
requires a new application major and a documented transition procedure. A major
bump itself does not provide that procedure or guarantee a zero-downtime transition.

This decision establishes a development constraint. It adds no runtime bypass,
capability negotiation or automatic token-accounting transition. Existing v0.7.0
can be the starting point without changing its coordination implementation.

## What must remain compatible

- **Fingerprint:** identical effective settings must produce identical bytes.
  PolicyFingerprint currently hashes json.Marshal(ReplicaPolicy), including
  nested circuit/tokenization structs. Field additions, names, order, tags,
  serialization and default changes can all break this rule. Freeze that wire
  representation; an internal refactor must preserve its output.
- **Shared state:** preserve lease ownership/expiry, lock ordering, concurrency,
  priority reserve, RPM/soft-TPM, token reservations, circuit generations and
  probe permits. New and old readers, writers, renewers and cleanup workers must
  safely operate on the same records.
- **Replay:** preserve request fingerprints, terminal receipts, retention and
  idempotent completion. A successful registration alone is not evidence that
  request processing is compatible.
- **Configuration:** preserve the meaning of existing values and defaults.
  Application compatibility does not permit rolling changes to incompatible
  coordination settings. Keep those settings unchanged during a binary rollout.
- **Database:** preserve old-binary queries, writes, startup and rollback, not
  merely the existence of old columns. SQLite remains a single-process profile;
  this policy does not enable SQLite rolling overlap.

New features must either preserve these guarantees while old replicas serve, or
remain disabled until all serving replicas support them. Their later activation
must have its own documented behavior. In particular, v0.7.0 still rejects new
token-reserved requests with token_policy_transition while legacy leases remain
in the affected pool. This policy does not remove that barrier or authorize
silently discarding leases or accounting.

## Migration boundary

Store.Open currently invokes golang-migrate Up on every PostgreSQL startup.
That migration engine expects the database's current version to exist in the
binary's embedded migration source. An old binary can therefore fail to restart
after a new migration, even if its already-running process can still query the
expanded schema. v0.7.0 embeds migrations through 000007.

Consequently, adding 000008 is not automatically a compatible minor-release
change. Before advancing the schema, provide and verify a compatible startup
and rollback path for the supported old binaries. A staged compatibility release
may be needed; every overlap stage must keep its old binaries restartable. Do
not advertise a required multi-step route as a direct rolling upgrade from every
0.x version. If no compatible route is available, defer the migration or treat
the change as a major-version transition.

Use additive expansion followed by later cleanup where appropriate, but also
check DDL lock duration under load. Do not rewrite already released migrations
or run destructive down migrations as part of a rolling binary rollback.

## Deployment consequences

For releases satisfying this policy, use the
[compatible rolling deployment procedure](deployment.md#compatible-postgresql-rolling-updates).
The registration guard already allows replicas with the same policy fingerprint;
the binary version is informational and does not enter that hash.

Keep LLMGW_COORDINATION_RESET_ON_START=false. Keep coordination-critical settings
and request-size feature activation separate from the binary update. Wait for
the replacement's coordination and inference readiness before draining an old
replica. Preserve lease renewal through HTTP drain and budget for long SSE,
completion flushing and load-balancer propagation.

The v0.6.x → v0.7.0 contract change is not retroactively compatible. Follow the
existing incompatible-upgrade procedure for that transition. Likewise, changed
policy fingerprints still require their documented stop/start procedure unless
a separately implemented and tested transition supports them.

## Review and release evidence

For changes touching coordination or persistence, compare the affected supported
old and new binaries on one PostgreSQL database. Verify admission, renewals,
completion/replay, circuits, recovery and cleanup as relevant, plus old-process
restart and rollback after migrations. Verify fingerprint stability against
the baseline serialization; hashing one current struct twice is insufficient.

For a zero-downtime deployment claim, additionally test the actual ingress and
orchestrator under continuous requests and long streams. Protocol compatibility
does not by itself prove that readiness propagation, shutdown budgets or DDL
locks will avoid errors. Documentation/comment-only changes do not require
introducing a new test suite.
