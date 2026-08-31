[English](local-demo.md) | [Русский](local-demo.ru.md)

# Локальный demo с двумя реальными vLLM

Этот воспроизводимый сценарий запускает опубликованный артефакт vLLM Priority Gateway перед двумя реальными `vllm/vllm-openai:v0.28.0` с моделью `Qwen/Qwen3-0.6B`. Выберите GHCR-образ или Linux-бинарник с проверенным checksum. Gateway не собирается из checkout.

Проверенные параметры запускают оба небольших vLLM на одной NVIDIA GPU с минимум 12 ГиБ VRAM. Среда проверяет routing, priority, overload, метрики, релизные артефакты и Admin UI, но не является рекомендуемой production-топологией или калибровкой production latency.

## Требования

- Docker Engine или Docker Desktop с Linux-контейнерами.
- Docker Compose 2.24.4 или новее.
- NVIDIA GPU, доступная Docker, с [Compose GPU reservations](https://docs.docker.com/compose/how-tos/gpu-support/).
- Около 25 ГиБ свободного места для vLLM-образа, весов модели и кэшей.
- Доступ к Hugging Face для публичной модели Qwen; `HF_TOKEN` необязателен.
- Для native-пути: Linux `amd64` или `arm64`, `curl`, `tar` и GNU `sha256sum`.

## 1. Подготовьте секреты и проверьте Docker

Клонируйте репозиторий и создайте `.env` на основе [`.env.example`](../.env.example). Заполните два пустых секрета независимо сгенерированными случайными значениями:

```dotenv
LLMGW_VERSION=0.2.0
LLMGW_ADMIN_USERNAME=operator
LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-random-bytes
LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
LLMGW_PORT=8080
```

`LLMGW_VERSION` — версия образа или архива без начальной `v`. Ограничьте доступ к `.env` операторской учётной записью и не добавляйте файл в Git. Остальные значения фиксируют проверенные vLLM-образ, модель, memory fraction и compatibility runner.

Проверьте конфигурацию и GPU:

```console
docker compose config --quiet
docker compose run --rm --no-deps --entrypoint nvidia-smi vllm-a
```

Первая команда должна завершиться без вывода, вторая — показать нужную NVIDIA GPU из контейнера vLLM.

## 2. Выберите один артефакт gateway

Не запускайте оба варианта одновременно: оба слушают `127.0.0.1:8080`. Их SQLite-состояние независимо.

### Вариант A: GHCR-образ

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.release.yaml pull gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml up -d --no-build --wait --wait-timeout 900
docker compose --env-file .env -f compose.yaml -f compose.release.yaml ps
```

`compose.release.yaml` выбирает `ghcr.io/rislanov/vllm-priority-gateway:${LLMGW_VERSION}` и отключает локальную сборку gateway. На чистом хосте загрузка образов и первый запуск модели могут занять несколько минут.

Сервис `probe` предоставляет curl внутри приватной Compose-сети:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/healthz
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/readyz
```

`/readyz` проверяет management plane и намеренно доступен до настройки model pool.

### Вариант B: native Linux-бинарник с проверенным checksum

Запустите только два inference-сервера. `compose.native.yaml` публикует их порты на loopback:

```console
docker compose --env-file .env -f compose.yaml -f compose.native.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.native.yaml up -d --wait --wait-timeout 900 vllm-a vllm-b
docker compose --env-file .env -f compose.yaml -f compose.native.yaml ps
```

Скачайте архив для текущей архитектуры и проверьте его до распаковки:

```bash
VERSION=0.2.0
case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

ASSET="vllm-priority-gateway_${VERSION}_linux_${ARCH}.tar.gz"
BASE_URL="https://github.com/rislanov/vllm-priority-gateway/releases/download/v${VERSION}"
RELEASE_DIR="$PWD/dist/release-${VERSION}"
mkdir -p "$RELEASE_DIR" &&
curl --fail --location --remove-on-error --output "$RELEASE_DIR/$ASSET" "$BASE_URL/$ASSET" &&
curl --fail --location --remove-on-error --output "$RELEASE_DIR/SHA256SUMS" "$BASE_URL/SHA256SUMS" &&
(cd "$RELEASE_DIR" && sha256sum --check --ignore-missing SHA256SUMS) &&
tar --extract --gzip --file "$RELEASE_DIR/$ASSET" --directory "$RELEASE_DIR"
```

Checksum-команда должна вывести `./$ASSET: OK`. Запустите gateway на переднем плане и оставьте терминал открытым:

```bash
install -d -m 0750 "$PWD/data/release-linux"
set -a
. ./.env
set +a
export LLMGW_LISTEN_ADDRESS=127.0.0.1:8080
export LLMGW_DATABASE_PATH="$PWD/data/release-linux/llmgw.db"
"$RELEASE_DIR/gateway"
```

## 3. Настройте gateway

Откройте `http://127.0.0.1:8080/admin` и войдите с Admin credentials из `.env`.

Создайте сущности в следующем порядке:

1. **Backends → Create model pool**
   - Public model name: `qwen`
   - Upstream model name: `qwen-test`
   - Max gateway inflight: `32`
   - Max waiting: `8`
   - Enabled: включено
2. Создайте два backend в одном пуле:
   - GHCR gateway: `vllm-a` → `http://vllm-a:8000`; `vllm-b` → `http://vllm-b:8000`
   - Native gateway: `vllm-a` → `http://127.0.0.1:8001`; `vllm-b` → `http://127.0.0.1:8002`
   - Capacity hint: `1`; running soft limit: `1`; enabled: включено
3. **Clients → Create client**
   - Name: `production-app`
   - Priority class: `normal`
   - vLLM priority: `0`
   - Max concurrency: `8`
   - Model access: `qwen`
4. **API Keys → Generate API key**
   - Выберите `production-app`
   - Сразу скопируйте полный `llmgw_*`: секрет показывается один раз.

## 4. Проверьте реальный inference

Дождитесь двух healthy backend со свежими метриками. Для GHCR-пути:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/inference-readyz
```

Для native Linux:

```console
curl -fsS http://127.0.0.1:8080/inference-readyz
```

Продолжайте, когда ответ содержит `"backendAvailability":2`. Подставьте одноразовый ключ клиента и запросите список моделей:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/v1/models -H "Authorization: Bearer llmgw_replace-with-the-generated-key"
```

Отправьте сохранённый streaming-запрос:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS -N http://gateway:8080/v1/chat/completions -H "Authorization: Bearer llmgw_replace-with-the-generated-key" -H "Content-Type: application/json" --data-binary "@/requests/chat.json"
```

Для native Linux:

```bash
export LLMGW_CLIENT_KEY='llmgw_replace-with-the-generated-key'
curl -fsS -N http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  -H "Content-Type: application/json" \
  --data-binary "@examples/quickstart-chat.json"
```

Поток должен завершиться `data: [DONE]`. Gateway аутентифицирует публичную модель `qwen`, применяет сохранённый priority, переписывает имя на `qwen-test` и выбирает здоровый vLLM.

## 5. Decision telemetry

Добавьте observability overlay для Prometheus и Grafana:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml -f compose.observability.yaml up -d --no-build --wait --wait-timeout 900
```

Grafana доступна по адресу `http://127.0.0.1:3000/d/llmgw-gateway-decisions`. Локальные credentials по умолчанию — `admin` / `admin`; измените их перед любым использованием вне loopback. Dashboard разделяет admission и latency: показывает, остаётся ли High traffic admitted, не утверждая, что latency перегруженной GPU неизменна.

## 6. Restart и cleanup

Перезапустите только gateway и убедитесь, что Admin configuration и inference readiness восстановились:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml restart gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/inference-readyz
```

Для GHCR-пути:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml logs -f gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml down
```

Для native Linux остановите foreground gateway с помощью `Ctrl-C`, затем выполните:

```console
docker compose --env-file .env -f compose.yaml -f compose.native.yaml down
```

Native SQLite остаётся в `data/release-linux`; Docker volumes сохраняют state gateway и cache модели. Добавляйте `--volumes` только при намеренном удалении данных.

## Evidence и production

Записанные результаты RTX 4070 Ti находятся в [acceptance-evidence.md](acceptance-evidence.md). Более широкие процедуры compatibility, cancellation, priority, resilience и сохранения артефактов описаны в [real-gpu-testing.md](real-gpu-testing.md) и [real-vllm-priority-e2e.md](real-vllm-priority-e2e.md).

Это development-sized validation environment. Перед публикацией gateway или использованием production-модели прочитайте [deployment.md](deployment.md) и [operations.md](operations.md).
