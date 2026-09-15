#!/bin/sh
set -eu

container="llmgw-postgres-test-$$"
cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run -d --name "$container" -e POSTGRES_PASSWORD=llmgw -e POSTGRES_DB=llmgw_test -p 127.0.0.1::5432 postgres:16-alpine >/dev/null
attempt=0
until docker exec "$container" pg_isready -U postgres -d llmgw_test >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    printf '%s\n' 'PostgreSQL 16 test container did not become ready' >&2
    exit 1
  fi
  sleep 1
done
port=$(docker port "$container" 5432/tcp | sed 's/.*://')
LLMGW_POSTGRES_TEST_DSN="postgres://postgres:llmgw@127.0.0.1:${port}/llmgw_test?sslmode=disable" make test-postgres
