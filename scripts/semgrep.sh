#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$script_dir/.."

# This is a Go html/template application. Replace only Django's CSRF syntax
# check with the tested rule for the request-bound .CSRF / $.CSRF input.
exec "${SEMGREP:-semgrep}" scan --oss-only \
  --config p/default \
  --config p/security-audit \
  --config .semgrep/go-templates.yml \
  --exclude-rule python.django.security.django-no-csrf-token.django-no-csrf-token \
  --metrics off --disable-version-check "$@"
