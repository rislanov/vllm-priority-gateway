[English](README.md) | [Русский](README.ru.md)

# vLLM Priority Gateway

[![CI](https://github.com/rislanov/vllm-priority-gateway/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/rislanov/vllm-priority-gateway/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/rislanov/vllm-priority-gateway)](https://github.com/rislanov/vllm-priority-gateway/releases/latest)
[![Go 1.27](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](go.mod)
[![License: Unlicense](https://img.shields.io/badge/license-Unlicense-blue.svg)](LICENSE)

**Защищает высокоприоритетные inference-нагрузки в общих GPU-кластерах vLLM.**

vLLM Priority Gateway — лёгкий слой policy, admission и routing перед **существующими серверами [vLLM](https://docs.vllm.ai/)**. Приложения продолжают использовать стандартный OpenAI API: достаточно изменить base URL, использовать выданный gateway ключ и оставить развёртывание моделей и GPU scheduling существующей инфраструктуре.

**Проверено на реальном vLLM с GPU NVIDIA**, включая saturation, изоляцию приоритетов, отказ и восстановление backend-серверов, streaming и сохранность состояния после перезапуска. [Результаты проверки](docs/acceptance-evidence.md).

## Какую проблему решает

Обычный HTTP load balancer распределяет запросы, но обычно не знает:

- какой клиент может потреблять общую GPU capacity;
- какой запрос относится к production, а какой — к background;
- сколько запросов выполняется и ожидает внутри scheduler каждого vLLM;
- насколько занят KV cache;
- когда нужно первым отклонить низкоприоритетный трафик;
- какой backend лучше сохраняет prefix-cache locality.

Gateway объединяет server-side policy клиента с live health и Prometheus-метриками vLLM:

```text
production requests  ── high ────────┐
interactive agents   ── normal ──────┼──► общая vLLM capacity
nightly / eval jobs  ── background ──┘

                         GPU pressure растёт

background traffic ──► throttled / 429
production traffic ──► остаётся admitted
```

## Место в кластере

```text
                    Существующая inference-инфраструктура

 Production API ────────┐
                        │  llmgw_prod_*     priority: high
 Coding agents ─────────┼──────────────────────────────────┐
                        │  llmgw_agents_*   priority: normal
 Batch / eval jobs ─────┘  llmgw_batch_*    priority: background
                                                            │
                                                            ▼
                                              ┌───────────────────────┐
                                              │ vLLM Priority Gateway │
                                              │ auth / policy         │
                                              │ admission control     │
                                              │ pressure-aware route  │
                                              │ session affinity      │
                                              │ circuit breakers      │
                                              └───────────┬───────────┘
                                                          │
                                      ┌───────────────────┼───────────────────┐
                                      ▼                   ▼                   ▼
                               vLLM GPU node A     vLLM GPU node B     vLLM GPU node C
```

Gateway **не** развёртывает модели, не планирует GPU и не заменяет Kubernetes, Slurm или существующие lifecycle-инструменты vLLM. Он решает, **кто получает общую inference capacity и на какой экземпляр vLLM направить запрос**.

Для развёртывания нужны один статический Go-бинарник и каталог SQLite. Текущая версия намеренно рассчитана на один gateway и небольшой пул backend-серверов под управлением оператора.

## Инженерные особенности

- Server-side priority: входящие priority headers и JSON-поля удаляются и заменяются сохранённой policy клиента.
- Явные model ACL и per-client concurrency limits.
- Независимый health/metrics polling backend-серверов, EWMA pressure и hysteretic pool states.
- Least-pressure routing, мягкая session affinity, drain и один консервативный retry до первого байта.
- Pool-wide safety limits и per-backend circuit breakers.
- Немедленный streaming и propagation отмены downstream-клиента.
- Metadata-only аналитика запросов и токенов без сохранения prompts и сгенерированного текста.
- Встроенные Basic-authenticated, CSRF-protected Admin UI и JSON Admin API.
- Prometheus, структурированные completion logs и готовый causal Grafana dashboard.

## Быстрый старт за 5 минут

Этот путь подключает опубликованный gateway к уже работающим vLLM. Gateway должен обращаться к `/health`, `/metrics` и `/v1/*` каждого зарегистрированного сервера.

1. Создайте `.env` с независимо сгенерированными секретами:

   ```dotenv
   LLMGW_ADMIN_USERNAME=operator
   LLMGW_ADMIN_PASSWORD=replace-with-at-least-16-random-bytes
   LLMGW_API_KEY_HMAC_SECRET=replace-with-at-least-32-random-bytes
   ```

2. Запустите опубликованный образ с постоянным SQLite volume:

   ```console
   docker pull ghcr.io/rislanov/vllm-priority-gateway:0.3.0
   docker volume create llmgw-data
   docker run -d --name vllm-priority-gateway --restart unless-stopped -p 127.0.0.1:8080:8080 --env-file .env -v llmgw-data:/data ghcr.io/rislanov/vllm-priority-gateway:0.3.0
   ```

3. Откройте `http://127.0.0.1:8080/admin` и создайте:

   - model pool с публичным и upstream именем модели;
   - каждый экземпляр vLLM как отдельный backend;
   - клиента с priority, concurrency и model access;
   - API-ключ клиента. Скопируйте его сразу: полный секрет показывается один раз.

4. Переключите OpenAI-клиент на gateway:

   ```python
   from openai import OpenAI

   client = OpenAI(
       base_url="http://127.0.0.1:8080/v1",
       api_key="llmgw_...",
   )

   response = client.chat.completions.create(
       model="your-public-model",
       messages=[{"role": "user", "content": "Review this function."}],
   )
   ```

Для автономной GPU-среды с двумя реальными vLLM, проверки checksum релиза, точных Admin-значений, inference probes и cleanup используйте [полный локальный real-vLLM demo](docs/local-demo.ru.md).

## Admin UI

UI встроен в gateway; отдельное frontend-развёртывание не требуется.

### Состояние gateway и backend-серверов

![Dashboard gateway со здоровым пулом и backend](docs/images/admin-dashboard.jpg)

### Выдача и отзыв API-ключей

![Список API-ключей и форма выдачи](docs/images/admin-api-keys.jpg)

### Metadata-only аналитика

![Графики запросов, токенов и cache usage](docs/images/admin-analytics.jpg)

## Клиентский API

```text
GET  /v1/models
POST /v1/chat/completions
POST /v1/completions
POST /v1/responses
```

Для prefix-cache locality передавайте один непрозрачный `X-LLM-Session-Id` в последовательных запросах агента или диалога. Значение ограничено, не попадает в логи и metric labels и удаляется перед forwarding. Health, drain, свежесть метрик, circuit state и pressure всегда важнее affinity.

## Production и эксплуатация

- [Production deployment](docs/deployment.md): Docker/systemd, TLS reverse proxy, secrets, backup и restore.
- [Operations guide](docs/operations.md): readiness, routing, drain/resume, circuits, pool safety, метрики, логи и recovery.
- [Real-vLLM E2E runbook](docs/real-vllm-priority-e2e.md): smoke, priority, saturation, drain и resilience.
- [Real-GPU validation](docs/real-gpu-testing.md): compatibility и калибровка порогов на целевом железе.

| Endpoint | Назначение |
|---|---|
| `/healthz` | Liveness процесса |
| `/readyz` | Готовность SQLite и registry |
| `/inference-readyz` | Доступная inference capacity; HTTP `503`, если её нет |
| `/metrics` | Prometheus telemetry |
| `/admin` | Интерфейс оператора |

Observability overlay запускает Prometheus и добавляет dashboard **Gateway Decisions**:

```console
docker compose --env-file .env -f compose.yaml -f compose.observability.yaml up -d --build --wait --wait-timeout 900
```

Dashboard показывает pressure пула → точные причины Low 429 → остаётся ли High admitted → ожидание High внутри gateway. Он не называет latency перегруженной GPU стабильной.

## Тестирование и проверка на реальном vLLM

Релизные артефакты и поведение при перегрузке проверены на реальной inference-среде:

- NVIDIA RTX 4070 Ti с 12 ГБ VRAM;
- два `vllm/vllm-openai:v0.28.0` с моделью `Qwen/Qwen3-0.6B`;
- saturation при `GatewayInflight=16` и `TotalWaiting=14`;
- низкоприоритетные probes отклонялись, а High/Critical продолжали admission;
- проверены backend drain, pool safety limits, открытие и восстановление circuit breaker;
- streaming, restart gateway и сохранность SQLite проверены на реальном vLLM;
- propagation отмены downstream-клиента покрыта детерминированными integration tests;
- отдельный Prometheus/Grafana-сценарий воспроизвёл цепочку pressure → shedding → admission.

Под записанной нагрузкой защищённый High-запрос продолжал admission, однако latency первого байта выросла примерно с `187ms` до `561ms`. Evidence доказывает изоляцию admission, а не неизменность GPU latency при saturation.

- [Acceptance evidence](docs/acceptance-evidence.md) связывает claims с automated tests и записанными hardware runs.
- [Automated real-vLLM E2E](docs/real-vllm-priority-e2e.md) содержит opt-in deployed tests.
- [Real-GPU validation](docs/real-gpu-testing.md) определяет retained evidence и production calibration.

```bash
make test
make test-race
make vet
make build
make container-smoke  # нужен Docker
```

## CI и релизы

Для pull request и push в `main` параллельно запускаются unit tests, Go race detector, `go vet`, сборка бинарника gateway и Docker-образа. Агрегированный status **Unit tests and builds** становится успешным только после всех пяти jobs и используется как required branch check. CI не публикует артефакты.

Release workflow запускается вручную, проверяет выбранный стабильный SemVer-тег, публикует Linux `amd64`/`arm64` архивы с checksum и multi-platform образ в GHCR. Использование артефактов описано в [Production deployment](docs/deployment.md), точный release-контракт — в [workflow](.github/workflows/release.yml).

## Текущие ограничения

- Только один экземпляр gateway; admission leases и runtime state backend-серверов находятся в памяти процесса.
- Статическая регистрация backend-серверов оператором; нет service discovery и autoscaling.
- Нет распределённых rate limits, token budgets, billing и GPU/NVML scheduling.
- Priority admission отклоняет новую низкоприоритетную работу, но не прерывает уже допущенную генерацию.
- Мягкая session affinity улучшает locality, не анализируя KV blocks и содержимое prefix.
- Для управления используется Basic auth. TLS, OIDC/RBAC, audit trail и secret manager должны предоставляться окружением.
- Capacity hints сохраняются для будущей совместимости, но пока не влияют на вес маршрутизации.

Более широкий целевой объём Production V1 описан в [технической спецификации](docs/technical-specification.md).

## Разработка

Требования: Go 1.27, macOS или Linux. SQLite-драйвер написан на чистом Go, поэтому CGO не требуется.

```bash
make test
make test-race
make vet
make build
make build-linux-amd64
make build-e2e-linux-amd64
```

## Документация

- [Полный локальный real-vLLM demo](docs/local-demo.ru.md)
- [Production deployment](docs/deployment.md)
- [Operations guide](docs/operations.md)
- [Technical specification](docs/technical-specification.md)
- [Acceptance evidence](docs/acceptance-evidence.md)
- [English README](README.md)

## Лицензия

[The Unlicense](LICENSE).
