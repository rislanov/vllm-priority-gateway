# Automatic coordination upgrade design and plan

Add a startup-only compatibility wait, defaulting to three minutes in the gateway.
Keep the low-level manager default fail-fast; only retry initial incompatible live
registrations. Preserve per-query timeouts, cancellation, and sticky runtime
incompatibility. Never delete foreign rows during waiting. Shutdown unregisters
only this process after HTTP/lease draining, before the database closes; bounded
failure leaves TTL fallback. Emergency reset remains opt-in and independent.

1. Add failing real PostgreSQL tests for expiry waiting, deadlines and graceful
   own-only unregister; test invalid configuration.
2. Implement manager waiting and context-bounded cleanup, config and app wiring.
3. Cover cancellation, sticky runtime conflict, fingerprint preservation, blocked
   cleanup and non-conflict errors; run focused PostgreSQL and race suites.
4. Document unreleased behavior and Recreate ownership, downtime, probe and timeout
   constraints in both READMEs and operations/deployment guides; update env/Compose.
5. Run make test, go vet and independent review. Do not push, merge or release.
