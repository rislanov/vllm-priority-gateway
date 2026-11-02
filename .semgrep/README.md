# Security scans

Install Semgrep CE with `uv tool install semgrep`, then run:

- `make semgrep-templates`: test the local rule and require zero template findings.
- `make semgrep`: test the local rule and scan the project.
- `scripts/semgrep.sh --json-output /tmp/semgrep.json .`: export a full scan.

The scanner keeps `p/default` and `p/security-audit`. It replaces only the
Django-specific CSRF rule with `go-template-mutation-form-missing-csrf`, because
this project uses Go `html/template` and a request-bound `.CSRF` / `$.CSRF` value.
No `nosemgrep` annotations or template-file exclusions are used.

The local rule checks mutation forms for an input named `csrf_token` containing
that actual template value; missing, empty and static tokens are reported. Its
fixtures include valid GET forms, both root/scoped tokens, attribute ordering,
quote styles and mutation forms longer than ten lines. This static check does
not replace the server's CSRF verification or the rendering/browser tests.

Semgrep CE's HTML parser may still emit partial-parsing warnings for Go template
directives. Generic template rules and the local CSRF rule still run. A scan
without findings is not a proof of complete vulnerability coverage.
