#!/bin/sh
set -eu

image="llmgw-container-smoke-$$"
container="llmgw-container-smoke-$$"
volume="llmgw-container-smoke-$$"

cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
  docker image rm "$image" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker build --platform linux/amd64 -t "$image" .
docker volume create "$volume" >/dev/null
docker run -d --name "$container" -p 127.0.0.1::8080 -v "$volume:/data" \
  -e LLMGW_ADMIN_USERNAME=operator \
  -e LLMGW_ADMIN_PASSWORD='correct horse battery staple' \
  -e LLMGW_API_KEY_HMAC_SECRET='01234567890123456789012345678901' \
  "$image" >/dev/null

port="$(docker port "$container" 8080/tcp | sed -n 's/.*://p' | head -n 1)"
attempt=0
while [ "$attempt" -lt 50 ]; do
  if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null; then
    exit 0
  fi
  attempt=$((attempt + 1))
  sleep 0.1
done

docker logs "$container"
exit 1
