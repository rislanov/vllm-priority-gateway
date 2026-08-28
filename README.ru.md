[English](README.md) | [Русский](README.ru.md)

# Lightweight vLLM Priority Gateway

## Содержание

- [Что это за продукт](#что-это-за-продукт)
- [Основные возможности](#основные-возможности)
- [Быстрый запуск в production](#быстрый-запуск-в-production)
- [Админка](#админка)
- [Поведение клиентского API](#поведение-клиентского-api)
- [Эксплуатация и развёртывание](#эксплуатация-и-развёртывание)
- [Admin API](#admin-api)
- [Разработка и тестирование](#разработка-и-тестирование)
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

## Быстрый запуск в production

Этот сценарий подключает gateway к реальному vLLM и доводит настройку до первого авторизованного inference-запроса. Для gateway используется Docker. Нативная установка с systemd, требования к reverse proxy, production-проверки и backup/restore описаны в [подробном руководстве](docs/deployment.md).

### Требования

- Linux-хост с Docker для gateway.
- Production endpoint vLLM, доступный по приватной сети.
- Доверенный TLS reverse proxy перед публикацией gateway в клиентскую сеть.
- `curl`; для проверок также удобен `jq`.

В примерах используются:

- upstream-модель: `Qwen/Qwen2.5-7B-Instruct`;
- публичная модель для клиентов: `qwen`;
- приватный URL vLLM, доступный из контейнера gateway: `http://vllm.internal:8000`;
- адрес gateway на Docker-хосте: `http://127.0.0.1:8080`.

Замените все четыре значения под своё окружение. Важно: `127.0.0.1` внутри контейнера gateway указывает на сам контейнер, а не на процесс vLLM на Docker-хосте.

### 1. Запустите vLLM с priority scheduling

Если vLLM уже управляется вашей inference-платформой, проверьте эквивалентность параметров и переходите к следующему шагу.

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --host 0.0.0.0 \
  --port 8000 \
  --scheduling-policy priority \
  --enable-prefix-caching \
  --enable-prompt-tokens-details \
  --enable-request-id-headers
```

В vLLM меньшие целочисленные значения priority обслуживаются раньше. Флаг `--enable-prompt-tokens-details` позволяет строить аналитику cache-read, когда модель и backend возвращают эти данные. При фиксации или обновлении версии сверяйтесь с актуальными страницами [vLLM serve CLI](https://docs.vllm.ai/en/latest/cli/serve/) и [OpenAI-compatible server](https://docs.vllm.ai/en/latest/serving/online_serving/openai_compatible_server/).

Порт vLLM должен оставаться в приватной сети. Клиенты должны обращаться только к gateway.

### 2. Соберите и запустите gateway

```bash
git clone https://github.com/rislanov/vllm-priority-gateway.git
cd vllm-priority-gateway

umask 077
export LLMGW_ADMIN_PASSWORD="$(openssl rand -base64 24)"
export LLMGW_API_KEY_HMAC_SECRET="$(openssl rand -base64 48)"
# Сразу сохраните оба значения в утверждённом хранилище секретов.

docker build --platform linux/amd64 -t vllm-priority-gateway:local .
docker volume create llmgw-data
docker run -d --name llmgw --restart unless-stopped \
  -p 127.0.0.1:8080:8080 \
  -v llmgw-data:/data \
  -e LLMGW_ADMIN_USERNAME=operator \
  -e LLMGW_ADMIN_PASSWORD \
  -e LLMGW_API_KEY_HMAC_SECRET \
  vllm-priority-gateway:local
```

Проверьте, что процесс запущен, а SQLite и registry готовы:

```bash
curl -fsS http://127.0.0.1:8080/healthz | jq
curl -fsS http://127.0.0.1:8080/readyz | jq
```

`/readyz` проверяет management plane и намеренно остаётся доступным при нулевой inference capacity. `/inference-readyz` станет готов после настройки пула и успешных проверок backend.

### 3. Настройте gateway в админке

Откройте `http://127.0.0.1:8080/admin` через доступный только операторам маршрут и войдите как `operator`, используя `LLMGW_ADMIN_PASSWORD`.

Создайте сущности в следующем порядке:

1. Откройте **Backends → Create model pool**:
   - **Public model name:** `qwen`
   - **Upstream model name:** `Qwen/Qwen2.5-7B-Instruct`
   - **Max gateway inflight:** `32`
   - **Max waiting:** `8`
   - **Enabled:** включено
2. На той же странице создайте backend:
   - **Name:** `gpu-a`
   - **Model pool:** `qwen`
   - **URL:** `http://vllm.internal:8000`
   - **Capacity hint:** `1`
   - **Running soft limit:** `16`
   - **Enabled:** включено
3. Откройте **Clients → Create client**:
   - **Name:** `production-app`
   - **Priority class:** `normal`
   - **vLLM priority:** `0`
   - **Max concurrency:** `8`
   - **Model access:** отметьте `qwen`
   - **Enabled:** включено
4. Откройте **API Keys**, выберите `production-app`, при необходимости задайте срок действия и нажмите **Generate API key**. Сразу скопируйте полное значение `llmgw_*`: оно показывается только один раз.

Числовые лимиты выше — стартовый пример для проверки сценария, а не универсальный production sizing. Перед включением клиентского трафика откалибруйте их под модель, GPU, значение vLLM `--max-num-seqs`, требования к задержке и результаты saturation-тестов. Ноль отключает соответствующий лимит пула.

Если vLLM запущен с `--api-key`, в поле **Upstream API key environment variable** укажите только имя переменной gateway, например `VLLM_GPU_A_KEY`, и передайте эту переменную контейнеру. Не вставляйте сам upstream-секрет в Admin UI.

### 4. Проверьте capacity и отправьте реальные запросы

Дождитесь health- и metrics-проверок backend, затем проверьте inference readiness:

```bash
curl -fsS http://127.0.0.1:8080/inference-readyz \
  | jq -e '.status == "ready" and .backendAvailability > 0'
```

Экспортируйте одноразово показанный клиентский ключ:

```bash
export LLMGW_URL='http://127.0.0.1:8080'
export LLMGW_CLIENT_KEY='llmgw_copy-the-one-time-value-here'
```

Получите список доступных клиенту моделей:

```bash
curl -fsS "$LLMGW_URL/v1/models" \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" | jq
```

Отправьте потоковый Chat Completions запрос:

```bash
curl -fsS -N "$LLMGW_URL/v1/chat/completions" \
  -H "Authorization: Bearer $LLMGW_CLIENT_KEY" \
  -H 'X-LLM-Session-Id: production-agent-42' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "qwen",
    "messages": [{"role": "user", "content": "Explain priority scheduling in one sentence."}],
    "max_tokens": 64,
    "stream": true
  }'
```

Клиент отправляет публичное имя `qwen`. Gateway проверяет ключ, применяет политику клиента, заменяет модель на `Qwen/Qwen2.5-7B-Instruct`, устанавливает управляемый сервером приоритет vLLM, выбирает подходящий backend и передаёт потоковый ответ.

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

В репозитории также есть детерминированный fake vLLM и генератор нагрузки. Это инструменты разработки и тестирования, а не часть production quick start.

Реализация и детерминированный acceptance suite завершены. Опциональные real-vLLM режимы smoke, priority/pool-safety и circuit-resilience прошли на Apple M4 с vLLM-Metal 27 августа 2026 года. Перед production sign-off повторите compatibility, saturation и threshold calibration на выбранной модели и GPU.

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
