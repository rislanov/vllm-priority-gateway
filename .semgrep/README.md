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

Template files are HTML fragments. The Go loader assigns the `layout` and
`content` names instead of embedding `define` directives at document level.
Document-level control directives in the editor fragments sit inside HTML
comments, which `html/template` removes from the response. Actual HTML elements
remain outside those comments and are still visible to the HTML rules.

`make semgrep-templates` uses `--strict`: parser warnings fail the check as well
as findings. Do not add rule exclusions or ignore parser errors to make it pass.
Rendering/browser tests protect the resulting DOM and form behavior. A scan
without findings is not a proof of complete vulnerability coverage.
