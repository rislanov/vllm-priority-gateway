[English](README.md) | [Русский](README.ru.md)

# Lightweight vLLM Priority Gateway

## Содержание

- [Что это за продукт](#что-это-за-продукт)
- [Основные возможности](#основные-возможности)
- [Быстрый старт из релиза](#быстрый-старт-из-релиза)
- [Админка](#админка)
- [Поведение клиентского API](#поведение-клиентского-api)
- [Эксплуатация и развёртывание](#эксплуатация-и-развёртывание)
- [Admin API](#admin-api)
- [Разработка и тестирование](#разработка-и-тестирование)
- [CI и релизы](#ci-и-релизы)
- [Текущие ограничения](#текущие-ограничения)
- [Документация](#документация)

## Что это за продукт

Lightweight vLLM Priority Gateway — это компактный слой управления и маршрутизации перед статическим пулом OpenAI-совместимых серверов [vLLM](https://docs.vllm.ai/). Приложения продолжают использовать обычные OpenAI-клиенты, а операторы централизованно задают приоритет, конкурентность, доступ к моделям, правила маршрутизации и поведение при перегрузке.

Без gateway каждый клиент обращается к vLLM напрямую и потенциально может назначить себе приоритет. С gateway клиент получает API-ключ с ограниченными правами, запрашивает публичное имя модели и не может повысить собственный приоритет. Gateway заменяет модель и приоритет, применяет admission policy, выбирает здоровый backend по текущей нагрузке GPU и сразу начинает передавать потоковый ответ клиенту.

```text
OpenAI-клиент
    │ Bearer-ключ llmgw_*
    ▼
┌──────────────────────────────────────────────────────────┐
│ Lightweight vLLM Priority Gateway                       │
│ auth → доступ к модели → admission → affinity/routing   │
│ streaming proxy │ health/pressure │ Admin UI/API        │
│ analytics       │ Prometheus      │ SQLite registry     │
└───────────────────────────┬──────────────────────────────┘
                            │ управляемый X-Vllm-Priority
                   ┌────────┴────────┐
                   ▼                 ▼
              vLLM backend A    vLLM backend B
```

Для развёртывания нужны один статический Go-бинарник и один каталог состояния SQLite. Текущая версия намеренно рассчитана на один экземпляр gateway и небольшой пул backend-серверов под управлением оператора. Redis, PostgreSQL, message broker, Kubernetes controller и отдельная сборка frontend не требуются.

## Основные возможности

- OpenAI-совместимые `GET /v1/models`, `POST /v1/chat/completions`, `POST /v1/completions` и `POST /v1/responses`.
- Криптографически стойкие клиентские ключи, которые хранятся только как HMAC-SHA-256 digest, а не в открытом виде.
- Для каждого клиента: включение/отключение, класс приоритета, целочисленный приоритет vLLM, лимит конкурентности и явный доступ к моделям.
- Отдельные публичные и upstream-имена моделей, статические пулы из нескольких backend-серверов.
- Независимые health- и Prometheus-проверки backend-серверов, EWMA pressure и устойчивые к дребезгу состояния пула.
- Priority-aware admission, ограниченные ответы `429`, маршрутизация на наименее загруженный backend, мягкая session affinity, drain и одна консервативная повторная попытка до получения заголовков.
- Circuit breaker для каждого backend и лимиты gateway-inflight/waiting для каждого пула.
- Немедленный streaming и передача отмены запроса upstream-серверу.
- Аналитика запросов и токенов без сохранения контента: графики, фильтры, детализация запросов и CSV.
- Встроенные Admin UI и JSON Admin API с Basic auth и CSRF-защитой.
- Метрики Prometheus и структурированные JSON-логи завершённых запросов.

## Быстрый старт из релиза

Это канонический первый запуск опубликованного релиза. Выберите ровно один артефакт gateway: multi-platform образ из GHCR или Linux-бинарник из GitHub Release с проверкой checksum. Оба пути используют один `compose.yaml` для запуска двух реальных vLLM с `Qwen/Qwen3-0.6B`. Ни один release-путь не собирает gateway из checkout: checkout предоставляет только зафиксированную топологию vLLM и пример запроса.

Проверенные значения намеренно запускают два маломощных процесса vLLM на одной NVIDIA GPU с минимум 12 ГиБ VRAM. Они подходят для проверки маршрутизации и приоритетов, но не являются production sizing. Docker-путь gateway работает в Linux Docker Engine и Docker Desktop. Native-путь требует Linux `amd64` или `arm64`; локальные контейнеры vLLM публикуют порты только на loopback. Перед публикацией gateway за пределы хоста изучите [руководство по развёртыванию](docs/deployment.md).

### Требования

- Docker Engine или Docker Desktop с Linux-контейнерами и Docker Compose 2.24.4 или новее.
- NVIDIA GPU, доступная Docker, и daemon с поддержкой [GPU reservations в Compose](https://docs.docker.com/compose/how-tos/gpu-support/).
- Около 25 ГиБ свободного места для зафиксированного образа vLLM, весов модели и кэшей.
- Доступ к Hugging Face для публичной модели Qwen. `HF_TOKEN` необязателен, но снимает ограничения анонимной загрузки.
- Для native Linux-пути: `curl`, `tar` и GNU `sha256sum`.

### 1. Задайте локальные секреты и проверьте Docker

Клонируйте репозиторий и откройте его каталог. Создайте рядом с `compose.yaml` файл `.env` на основе [`.env.example`](.env.example), затем заполните оба пустых секрета независимо сгенерированными случайными значениями:

```dotenv
LLMGW_VERSION=0.1.0
LLMGW_ADMIN_USERNAME=operator
LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-random-bytes
LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
LLMGW_PORT=8080
```

`LLMGW_VERSION` — версия образа или архива без начальной `v`; соответствующий Git-тег — `v0.1.0`. Файл `.env` должен читаться только операторской учётной записью и не должен попадать в Git; репозиторий уже игнорирует его. Остальные значения в `.env.example` фиксируют проверенные образ vLLM, модель Qwen, долю памяти и compatibility runner. Меняйте параметры capacity только после первого успешного запуска.

Эти команды одинаковы в Bash и PowerShell:

```console
docker compose config --quiet
docker compose run --rm --no-deps --entrypoint nvidia-smi vllm-a
```

Первая команда должна завершиться без вывода. Вторая должна показать нужную NVIDIA GPU из зафиксированного контейнера vLLM.

### 2. Выберите один релизный артефакт

Не запускайте оба варианта gateway одновременно: оба слушают `127.0.0.1:8080`. Их SQLite-состояние независимо: Docker использует именованный volume, а native-путь — `data/release-linux`.

#### Вариант A: Docker-образ из GHCR

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.release.yaml pull gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml up -d --no-build --wait --wait-timeout 900
docker compose --env-file .env -f compose.yaml -f compose.release.yaml ps
```

`compose.release.yaml` отключает локальную Docker-сборку и выбирает `ghcr.io/rislanov/vllm-priority-gateway:${LLMGW_VERSION}`. Сначала `vllm-a` скачивает и компилирует модель; после его готовности запускается `vllm-b` и использует общий именованный Hugging Face cache. Релизный gateway запускается только после успешного `/health` обоих inference-серверов. На чистой машине загрузка образов и первый запуск модели могут занять несколько минут.

Опциональный сервис `probe` содержит зафиксированный curl-клиент внутри приватной Compose-сети. Поэтому host curl, jq, shell-переменные и platform-specific quoting не нужны:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/healthz
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/readyz
```

`/readyz` проверяет management plane и намеренно остаётся доступным до настройки пулов моделей.

#### Вариант B: native Linux-бинарник из GitHub Release

Запустите только два локальных inference-сервера. `compose.native.yaml` публикует их на loopback, чтобы процесс на хосте мог обращаться к ним без открытия vLLM для локальной сети:

```console
docker compose --env-file .env -f compose.yaml -f compose.native.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.native.yaml up -d --wait --wait-timeout 900 vllm-a vllm-b
docker compose --env-file .env -f compose.yaml -f compose.native.yaml ps
```

Скачайте архив для текущей Linux-архитектуры и до распаковки проверьте его по релизному manifest:

```bash
VERSION=0.1.0
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

Команда проверки должна вывести `./$ASSET: OK`. Запустите бинарник на переднем плане с отдельным каталогом SQLite; оставьте этот терминал открытым, а следующие шаги выполняйте во втором:

```bash
install -d -m 0750 "$PWD/data/release-linux"
set -a
. ./.env
set +a
export LLMGW_LISTEN_ADDRESS=127.0.0.1:8080
export LLMGW_DATABASE_PATH="$PWD/data/release-linux/llmgw.db"
"$RELEASE_DIR/gateway"
```

### 3. Настройте gateway в Admin

Откройте `http://127.0.0.1:8080/admin` и войдите с Admin username/password из `.env`.

Создайте сущности в следующем порядке:

1. Откройте **Backends → Create model pool**:
   - **Public model name:** `qwen`
   - **Upstream model name:** `qwen-test`
   - **Max gateway inflight:** `32`
   - **Max waiting:** `8`
   - **Enabled:** включено
2. Создайте два backend в одном пуле:
   - Docker gateway: **Name:** `vllm-a`; **URL:** `http://vllm-a:8000`, и **Name:** `vllm-b`; **URL:** `http://vllm-b:8000`
   - Native Linux gateway: **Name:** `vllm-a`; **URL:** `http://127.0.0.1:8001`, и **Name:** `vllm-b`; **URL:** `http://127.0.0.1:8002`
   - Для обоих вариантов: **Capacity hint:** `1`; **Running soft limit:** `1`; **Enabled:** включено
3. Откройте **Clients → Create client**:
   - **Name:** `production-app`
   - **Priority class:** `normal`
   - **vLLM priority:** `0`
   - **Max concurrency:** `8`
   - **Model access:** отметьте `qwen`
   - **Enabled:** включено
4. Откройте **API Keys**, выберите `production-app`, при необходимости задайте срок действия и нажмите **Generate API key**. Сразу скопируйте полное значение `llmgw_*`: оно показывается только один раз.

Docker gateway использует приватный DNS Compose. Native gateway использует loopback-порты из `compose.native.yaml`; ни один из вариантов не открывает неаутентифицированные endpoints vLLM для локальной сети.

### 4. Проверьте реальный inference

После опроса обоих backend readiness должен вернуть HTTP 200 и показать два доступных backend:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/inference-readyz
```

Для native Linux вместо этого выполните `curl -fsS http://127.0.0.1:8080/inference-readyz`. Дождитесь, пока ответ покажет `"backendAvailability":2`; перед отправкой автоматического трафика пропустите ещё один интервал мониторинга.

Подставьте одноразовый клиентский ключ вместо placeholder и получите список публичных моделей:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS http://gateway:8080/v1/models -H "Authorization: Bearer llmgw_replace-with-the-generated-key"
```

Для native Linux:

```bash
export LLMGW_CLIENT_KEY='llmgw_replace-with-the-generated-key'
curl -fsS http://127.0.0.1:8080/v1/models -H "Authorization: Bearer $LLMGW_CLIENT_KEY"
```

Отправьте потоковый запрос из [`examples/quickstart-chat.json`](examples/quickstart-chat.json):

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml run --rm --no-deps probe -fsS -N http://gateway:8080/v1/chat/completions -H "Authorization: Bearer llmgw_replace-with-the-generated-key" -H "Content-Type: application/json" --data-binary "@/requests/chat.json"
```

Для native Linux:

```bash
curl -fsS -N http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  -H "Content-Type: application/json" \
  --data-binary "@examples/quickstart-chat.json"
```

Поток должен завершиться строкой `data: [DONE]`. Клиент использует публичную модель `qwen`; gateway проверяет ключ, применяет сохранённый приоритет, заменяет upstream-модель на `qwen-test` и выбирает один из двух здоровых vLLM.

Для Docker-варианта:

```console
docker compose --env-file .env -f compose.yaml -f compose.release.yaml logs -f gateway
docker compose --env-file .env -f compose.yaml -f compose.release.yaml stop
docker compose --env-file .env -f compose.yaml -f compose.release.yaml down
```

Для native Linux остановите процесс gateway с помощью `Ctrl+C`, затем остановите vLLM:

```console
docker compose --env-file .env -f compose.yaml -f compose.native.yaml down
```

Обе команды `down` сохраняют именованный volume кэша модели; Docker-вариант также сохраняет volume SQLite. Для native-варианта SQLite остаётся в `data/release-linux`. Добавляйте `--volumes` только для намеренного удаления именованных volumes этого стека.

## Админка

Admin UI встроен в gateway, поэтому отдельное frontend-развёртывание не требуется. На скриншотах ниже используется синтетический трафик без production credentials.

### Состояние gateway и backend-серверов

Dashboard показывает готовность конфигурации, pressure и защитные лимиты пулов, а также health, circuit, running/waiting и KV-cache каждого backend.

![Dashboard с одним здоровым пулом и backend](docs/images/admin-dashboard.jpg)

### Выдача и отзыв API-ключей

Сначала создайте клиента и выдайте ему доступ к модели. Затем откройте **API Keys**, выберите клиента и необязательный срок действия, нажмите **Generate API key**. Полный секрет показывается один раз; в таблице остаются только prefix, status, expiry и время последнего использования. Отзыв применяется немедленно.

![Список API-ключей и форма выдачи](docs/images/admin-api-keys.jpg)

### Просмотр аналитики

После завершения inference-запросов откройте **Analytics**. Можно выбрать UTC preset или точный диапазон, отфильтровать данные по клиенту, модели и наличию usage, посмотреть графики запросов и токенов, изучить метаданные отдельных запросов или скачать тот же набор данных в CSV.

![Графики запросов, токенов и cache usage](docs/images/admin-analytics.jpg)

Аналитика хранит только операционные метаданные и количество токенов. Prompts, messages, сгенерированный текст, тела запросов и ответов, authorization headers и API-ключи не сохраняются.

## Поведение клиентского API

Поддерживаемые клиентские маршруты:

```text
GET  /v1/models
POST /v1/chat/completions
POST /v1/completions
POST /v1/responses
```

Клиент не может повысить собственный приоритет: gateway удаляет входящие `X-Vllm-Priority` и JSON-поле `priority`, заменяет публичное имя модели и применяет политику из SQLite. Неподдерживаемые `/v1/*` маршруты и ошибки gateway возвращаются в OpenAI-совместимом error envelope.

Для улучшения prefix-cache locality передавайте одинаковый непрозрачный `X-LLM-Session-Id` в последовательных запросах одного агента или диалога. Значение ограничено 256 байтами, не попадает в логи и metric labels и удаляется перед отправкой в vLLM. Health, drain, свежесть метрик, circuit state и текущий pressure всегда важнее affinity.

## Эксплуатация и развёртывание

- [Production deployment](docs/deployment.md): Docker и systemd, требования к reverse proxy, post-deployment verification, backup и restore.
- [Operations guide](docs/operations.md): readiness, routing, drain/resume, circuit breaker, pool safety, хранение аналитики, метрики, логи и границы безопасности.
- [Real-vLLM E2E runbook](docs/real-vllm-priority-e2e.md): безопасный для production smoke и изолированные priority, saturation, drain и resilience тесты.
- [Real-GPU test plan](docs/real-gpu-testing.md): финальная проверка совместимости модели/железа и калибровка порогов.

Основные runtime endpoints:

| Endpoint | Назначение |
|---|---|
| `/healthz` | Liveness процесса |
| `/readyz` | Готовность SQLite и registry |
| `/inference-readyz` | Доступная inference capacity; HTTP `503`, если её нет |
| `/metrics` | Метрики Prometheus |
| `/admin` | Интерфейс оператора |

Gateway применяет подтверждённые изменения Admin UI без перезапуска. Перед обслуживанием переведите backend в drain и возобновляйте его только после восстановления health и свежих метрик.

## Admin API

Все Admin-маршруты требуют Basic auth. Каждый запрос, меняющий состояние, дополнительно требует совпадающие double-submit CSRF cookie/header либо cookie/form token.

```text
GET    /admin/api/status
GET    /admin/api/clients
POST   /admin/api/clients
PUT    /admin/api/clients/{id}
POST   /admin/api/clients/{id}/keys
DELETE /admin/api/keys/{id}
GET    /admin/api/pools
POST   /admin/api/pools
PUT    /admin/api/pools/{id}
GET    /admin/api/backends
POST   /admin/api/backends
PUT    /admin/api/backends/{id}
POST   /admin/api/backends/{id}/drain
POST   /admin/api/backends/{id}/resume
GET    /admin/api/analytics
GET    /admin/api/analytics/requests
GET    /admin/api/analytics/export.csv
```

## Разработка и тестирование

Требования: Go 1.27, macOS или Linux. SQLite-драйвер написан на чистом Go, поэтому CGO не требуется.

```bash
make test
make test-race
make vet
make build
make build-linux-amd64
make build-e2e-linux-amd64
make container-smoke  # нужен запущенный Docker daemon
```

В репозитории также есть детерминированный fake vLLM и генератор нагрузки. Это инструменты разработки и тестирования, а не часть быстрого старта из релиза.

Реализация и детерминированный acceptance suite завершены. Local-build Docker-топология и real-vLLM режимы smoke, priority/pool-safety и circuit-resilience прошли 28 августа 2026 года на RTX 4070 Ti с двумя сервисами `Qwen/Qwen3-0.6B`. 30 августа 2026 года оба опубликованных пути v0.1.0 — GHCR-образ и Linux `amd64` архив с проверенным checksum — независимо выполнили реальный streaming inference через те же два vLLM и сохранили раздельное SQLite-состояние после перезапуска gateway. Перед production sign-off повторите compatibility, saturation и threshold calibration на выбранной production-модели и топологии.

## CI и релизы

Для pull request и push в `main` автоматически запускаются unit-тесты, `go vet`, сборка бинарника gateway и сборка Docker-образа. Артефакты и образы при этом не публикуются.

Релиз запускается вручную и использует самый новый существующий стабильный SemVer-тег, достижимый из `main`:

```bash
git tag -a v1.2.3 -m "v1.2.3"
git push origin v1.2.3
```

В GitHub откройте **Actions → Release → Run workflow**, укажите тег и запустите workflow. Релизы выполняются последовательно, а старый тег будет отклонён, чтобы он не мог откатить Docker-теги `latest`, major или minor. В GitHub Release будут опубликованы архивы gateway для Linux `amd64` и `arm64`, а также `SHA256SUMS`. Multi-platform образ получит имена `ghcr.io/rislanov/vllm-priority-gateway:1.2.3`, `:1.2`, `:1` и `:latest`. Номера версий всегда вычисляются только из выбранного тега.

Если запуск прервался после создания draft release, повторно запустите workflow с тем же тегом: он проверит и обновит соответствующий draft, затем продолжит публикацию. Настройте repository ruleset, запрещающий изменение и удаление стабильных тегов `v*`. Workflow проверяет remote tag непосредственно перед публикацией образа и ещё раз перед публикацией draft, но именно ruleset постоянно защищает тег от изменения во время релиза.

После первой публикации пакет в GitHub Container Registry может быть приватным. Если образ должен скачиваться без авторизации, один раз переключите visibility пакета на public в его настройках.

## Текущие ограничения

- Только один экземпляр gateway; admission leases и runtime state backend-серверов находятся в памяти процесса.
- Статическая регистрация backend-серверов оператором; нет service discovery и autoscaling.
- Нет распределённых rate limits, token budgets, billing и GPU/NVML scheduling.
- Priority admission отклоняет новую низкоприоритетную работу, но не прерывает уже допущенную генерацию.
- Мягкая session affinity улучшает locality, не анализируя KV blocks и содержимое prefix.
- Для управления используется Basic auth. TLS, OIDC/RBAC, audit trail и secret manager должны предоставляться окружением.
- Capacity hints сохраняются для будущей совместимости, но пока не влияют на вес маршрутизации.

Более широкий целевой объём Production V1 описан в [технической спецификации](docs/technical-specification.md).

## Документация

- [Production deployment](docs/deployment.md)
- [Operations guide](docs/operations.md)
- [Technical specification](docs/technical-specification.md)
- [Automated real-vLLM E2E](docs/real-vllm-priority-e2e.md)
- [Real-GPU test plan](docs/real-gpu-testing.md)
- [Acceptance evidence](docs/acceptance-evidence.md)
- [English README](README.md)
