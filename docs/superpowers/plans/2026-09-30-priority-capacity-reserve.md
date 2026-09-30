# Priority Capacity Reserve — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Реализовать issue #22: резервировать часть admission-ёмкости каждого пула для High/Critical, глобально между PostgreSQL-репликами и внутри единственной SQLite-реплики.

**Architecture:** Добавить настройку пула `HighPriorityReserve` и учитывать класс запроса на момент admission. PostgreSQL проверяет общий и низкоприоритетный inflight в существующей транзакции под блокировкой pool scope; local coordinator выполняет те же проверки под mutex. Использовать существующие leases, replay receipts, завершение и TTL, без второго механизма резервирования.

**Tech Stack:** Go 1.27.0, PostgreSQL 16+, pgx/v5, modernc SQLite, существующие chi Admin API, Go HTML templates, Prometheus и Grafana.

**Spec:** [GitHub issue #22](https://github.com/rislanov/vllm-priority-gateway/issues/22), прочитан 2026-09-30, OPEN, комментариев нет. Ниже сохранены требования и выбранные проектные решения; исходный план и фактические результаты исполнения приведены ниже.

**Base:** `origin/main`, `c48be28` — `chore: prepare v0.5.0 release (#21)`.

**Branch:** `codex/issue-22-priority-capacity-reserve`.

## Global Constraints

- High/Critical используют свободные слоты до общего лимита; Normal/Background совместно ограничены незарезервированной ёмкостью.
- Незанятый резерв недоступен Normal/Background. Borrowing, reclamation и preemption вне scope.
- Сохранить client concurrency, RPM/TPM, backend eligibility, pool waiting и общий inflight limit. Резерв не даёт обхода этих проверок.
- Нулевой резерв сохраняет прежнюю admission-политику. Положительный резерв допустим только с конечным положительным лимитом пула.
- Не вводить новые зависимости. Существующие пределы `MaxPoolGatewayInflight = 100_000`, `MaxClientConcurrency = 10_000` и SQLite grandfathering сохраняются.
- Admission reserve защищает слоты gateway, а не GPU memory, KV blocks или время исполнения. Уже допущенные генерации продолжаются.
- Degraded emergency caps остаются отдельными лимитами на процесс; глобальная гарантия резерва действует только при рабочей координации.

## Review Focus

1. Изменение класса клиента при активном запросе не переклассифицирует lease и не портит освобождение счётчиков — задачи 2–3.
2. Включение/увеличение резерва при уже занятой ёмкости запрещает новые низкоприоритетные admission до освобождения слотов, без отмены активных запросов — задачи 2–3.
3. Потеря ответа после COMMIT, повторный Complete и гонка renewal/expiry не создают второй lease и не освобождают слот дважды — задачи 2–3, 6.
4. Старые реплики, старые leases и устаревший snapshot не должны обходить резерв или выдавать повышенный приоритет — задачи 1, 3, 6.
5. При доступном резерве High/Critical всё равно получают отказ при total/client/rate/waiting/backend ограничениях; аварийный режим не считается глобальным резервом — задачи 3–4, 6.

## Требования issue и критерии готовности

| Требование | Реализация и проверка |
|---|---|
| Конфигурация пула, Admin API/UI, defaults и validation | 1, 5 |
| Атомарная проверка total и lower-priority inflight | 2, 3 |
| PostgreSQL global / SQLite single-replica | 2, 3, 6 |
| Completion, cancellation, crash, expiry, replay | 2, 3, 4, 6 |
| Отдельная наблюдаемая причина, прежний overload/backoff | 4 |
| Saturation, High/Critical, total cap, concurrent replicas, reserve=0 | 2, 3, 6 |
| Документация degraded coordination и emergency caps | 6 |
| Overload acceptance scenario с изоляцией admission | 6 |

## Контракт и принятые решения

### Настройка и алгоритм

`domain.ModelPool.HighPriorityReserve int`, `store.CreatePoolParams.HighPriorityReserve int` (Update — существующий alias), колонка `model_pools.high_priority_reserve`, JSON `highPriorityReserve`, HTML `high_priority_reserve`. Default — `0`.

Пусть `L = MaxGatewayInflight`, `R = HighPriorityReserve`, `T` — все активные leases пула, `B` — активные leases Normal/Background. При рабочей координации:

```text
valid: R >= 0 && (R == 0 || (L > 0 && R <= L))
all classes: if L > 0 && T >= L -> existing pool_inflight_limit
Normal/Background: if R > 0 && B >= L-R -> pool_priority_reserve
then existing client concurrency and RPM/TPM checks
allocate one lease only when every check passes
```

Использовать `B`, а не сравнение `T >= L-R`: например, при `L=20,R=6,T=14,B=4` ещё один Normal допустим. High/Critical могут занять все 20 слотов, если низкоприоритетных запросов нет. `R=L` допустим и запрещает новые Normal/Background. `L=0,R=0` сохраняет unlimited pool.

Класс берётся из аутентифицированного клиента, не из HTTP header/body или vLLM priority. В `AdmissionRequest` добавить `PriorityClass domain.PriorityClass` и `PoolHighPriorityReserve int`. В PostgreSQL сравнивать оба поля с заблокированной конфигурацией наряду с существующими revision/limit проверками. Неизвестный класс не получает резерв: вернуть `ReasonStaleConfiguration`; обновить старые внутренние test fixtures, задав им явный класс.

Учёт класса вести и при `R=0`, чтобы последующее включение резерва учитывало уже работающие запросы. Увеличение `R` или снижение `L` не прерывает leases: новые запросы допускаются только после удовлетворения текущих неравенств. Смена класса влияет только на будущие leases.

### Leases, миграции и совместимость

- PostgreSQL: `request_leases.priority_class TEXT NOT NULL DEFAULT 'normal' CHECK (...четыре класса...)`. Существующие leases консервативно считать низкоприоритетными; не восстанавливать их исторический класс из изменяемой таблицы clients.
- Local: хранить исходный класс в существующем `admissionReceipt.request`, добавить `poolLowerInflight map[int64]int`; decrement только в `removeLease`, по сохранённому receipt и только если lease действительно существовал. Внешний `LeaseIdentity` менять не требуется.
- PostgreSQL считать `T` и `B` по неистёкшим leases одним aggregate (`count(*)`, `count(*) FILTER (...)`) с одним `clock_timestamp()`. Сохранить отдельный SQL statement после ожидания scope lock: READ COMMITTED должен получить свежий snapshot.
- Сохранить lock order pool → client → leases/receipts/rates в существующих операциях. Total/reserve проверки идут до RPM debit; отказ не создаёт lease и не расходует RPM.
- Поля нового AdmissionRequest входят в существующий JSON fingerprint. Replay с тем же входом возвращает тот же результат; изменённый класс или резерв с тем же LeaseID — `ReasonIdempotencyConflict`. Replay уже принятого lease не перевычисляет решение по новой конфигурации.
- Поднять `CoordinationContractVersion` с `2` до `3`: новый формат admission и учёт классов несовместимы со старым исполнителем. Перезапуск с drain всех старых реплик обязателен; бесшовный mixed-version rollout не входит в эту фичу.
- PostgreSQL migration down допустима только после остановки трафика/реплик и сброса ненулевого резерва; не объявлять rollback работающего mixed-version кластера безопасным.

### Ошибки и интерфейс

Добавить `coordination.ReasonPoolPriorityReserve = "pool_priority_reserve"` и `gateway.DecisionPoolPriorityReserve` с тем же значением. Публично сохранить `429 gateway_overloaded`, существующий JSON envelope и `Retry-After`. В Prometheus `llmgw_requests_rejected_total` и structured log должна быть новая decision reason.

Для этого отдельного reason оставить `AdmissionDecision.ConcurrencyScope` пустым: существующий SQL constraint разрешает непустой scope только для `concurrency_exhausted`. Total exhaustion продолжает возвращать старый reason + pool scope. Replay сохраняет новый reason без изменения этого constraint.

Admin API остаётся с существующей полной заменой полей пула при PUT: отсутствующее `highPriorityReserve` означает `0`. Это сохраняет старые payload для конфигураций без резерва; клиент, редактирующий пул с резервом, должен передавать поле. Формы редактирования всегда отправляют текущее значение; это явно документировать и проверить.

## Основания в текущем коде

Проверка Tier 2 выполнена через graph discovery, trace и snippets; проект `C-Projects-vllm-priority-gateway`, generation `2026-09-28T16:57:44Z`. `check_index_coverage` отметил `metadata_changed` у проверенных файлов, поэтому существенные участки дополнительно проверены чтением/поиском в текущих исходниках. `deploy/` исключён из графа; Grafana query проверен непосредственно. Это task-directed проверка, не полный аудит репозитория.

- `cmd/gateway/application.go` создаёт local coordinator для SQLite и PostgreSQL coordinator для production profile.
- `internal/gateway/forward_admission.go:13` — передача admission request, reload при stale configuration и завершение lease. `Forward` вызывает этот метод.
- `internal/coordination/postgres/admission.go:129` — `acquire`: pool/client locks, чтение политики, count, rate checks, INSERT lease. `resolveAcquire`, `renew`, `completeOne` обеспечивают replay/lifecycle.
- `internal/coordination/local/admission.go:93` — `Acquire` под mutex; `removeLease` объединяет освобождение при Complete/expiry.
- `internal/store/postgres/configuration.go:181` — UpdatePool уже блокирует pool scope; конфигурационные изменения можно сериализовать с admission.
- `internal/gateway/service.go:323` — pool waiting/backend guards; при coordinator общий inflight проверяется внутри admission. Существует отдельный fallback без coordinator, который необходимо закрыть для положительного резерва.
- `internal/httpapi/admin.go`, `internal/web/resources.go`, `internal/web/templates/backends.html` — существующий путь создания/редактирования pool safety settings.
- `internal/observability/metrics.go:356` и `log.go` уже передают DecisionReason; Grafana dashboard использует allowlist причин и требует изменения.

## Порядок работ и файлы

Зависимости: **1 → 2 → 3 → 4 → 5 → 6**. Промежуточные коммиты не развёртывать как законченную фичу: настройки публикуются для эксплуатации после готовности обоих coordinators и acceptance suite.

### Задача 1: Модель настройки и хранение

**Modify:** `internal/domain/types.go`, `internal/domain/validate.go`, `internal/store/models.go`, `internal/store/pools.go`, `internal/store/sqlite.go`, `internal/store/postgres/configuration.go`.

**Create:** `internal/store/migrations/005_priority_capacity_reserve.sql`, `internal/store/postgres/migrations/000005_priority_capacity_reserve.up.sql`, `internal/store/postgres/migrations/000005_priority_capacity_reserve.down.sql`.

**Tests:** `internal/domain/validate_test.go`, `internal/store/sqlite_test.go`, `internal/store/crud_test.go`, `tests/postgres/migration_startup_test.go`, новый `tests/postgres/priority_reserve_configuration_test.go`.

**Interfaces:** Существующие `ModelPool.Validate() error`, `ValidateUpdate(previous ModelPool) error`, `CreatePool(ctx, CreatePoolParams) (domain.ModelPool, error)`, `UpdatePool(ctx, int64, UpdatePoolParams) (domain.ModelPool, error)` сохраняют сигнатуры. Новый `HighPriorityReserve int` проходит через create/update/list/snapshot/reopen.

- [ ] Написать `TestModelPoolPriorityReserveValidation` с таблицей `(L,R,valid)`: `(0,0,true)`, `(20,0,true)`, `(20,6,true)`, `(20,20,true)`, `(20,-1,false)`, `(20,21,false)`, `(0,1,false)`. Проверить Validate и ValidateUpdate, включая снижение L ниже R и сохранение SQLite grandfathered L.
- [ ] Добавить `TestSQLitePriorityReserveMigrationAndRoundTrip` и `TestPostgresPriorityReserveMigrationAndRoundTrip`: upgrade v4 → v5 даёт R=0; CRUD/reopen сохраняет R=6; invalid direct SQL запрещён; existing pool limits/revision не теряются; SQL backfill старого lease даёт `normal`; PostgreSQL down/up проверяется на остановленной тестовой БД.
- [ ] Запустить `go test ./internal/domain ./internal/store -run PriorityReserve -count=1` и PostgreSQL-тесты с заданным `LLMGW_POSTGRES_TEST_DSN`; убедиться, что новые проверки падают на отсутствии полей/миграций.
- [ ] Добавить поля, зарегистрировать SQLite migration 5, обновить SQL INSERT/UPDATE/SELECT/Scan и snapshot loaders. PostgreSQL добавить CHECK для R и конечного L; SQLite — CHECK нового столбца и insert/update triggers для отношения R к L, не удаляя grandfathering triggers. Добавить классификацию lease в PostgreSQL migration. Down удаляет только добавленную схему после защитной проверки R=0.
- [ ] Повторить новые тесты и `go test ./internal/domain ./internal/store ./internal/store/postgres`; PostgreSQL интеграцию запускать с DSN, SKIP не считать проверкой миграции.
- [ ] Commit: `feat: persist pool priority capacity reserve`.

### Задача 2: Контракт admission и local coordinator

**Modify:** `internal/coordination/types.go`, `internal/coordination/local/admission.go` и существующие factories AdmissionRequest в затронутых тестах.

**Create:** `internal/coordination/contracttest/priority_reserve.go`, `internal/coordination/local/priority_reserve_test.go`.

**Interfaces:** Новый reason и два поля AdmissionRequest из контракта выше. `Acquire(context.Context, coordination.AdmissionRequest) (coordination.AdmissionDecision, error)`, Renew и Complete сохраняют сигнатуры. Общий тестовый контракт: `PriorityReserve(t *testing.T, factory PriorityReserveFactory)`; `type PriorityReserveFactory func(t *testing.T, limit, reserve int) (coordination.AdmissionCoordinator, func(domain.PriorityClass) coordination.AdmissionRequest)`. Factory создаёт клиентов всех четырёх классов с concurrency >=20 и отключёнными RPM/TPM, перед выдачей запросов фиксирует актуальную revision.

- [ ] Написать общий `PriorityReserve` contract: L=20/R=6, 7 Normal + 7 Background приняты, следующий запрос каждого low класса отклонён с новым reason; 3 High + 3 Critical приняты, 21-й запрос любого класса отклонён total limit. Отдельно: high-first `T=14,B=4`, R=0, unlimited `(0,0)`, R=L, независимые пулы и несколько клиентов одного класса.
- [ ] Написать local tests `TestPriorityReserveReleaseAndExpiry`, `TestPriorityReserveReplay`, `TestPriorityReservePolicyChanges`, `TestPriorityReserveConcurrentAcquire`: Complete дважды меняет счётчики один раз; expiry возвращает ёмкость; renewal удерживает её; lease replay не увеличивает T/B; rejected replay сохраняет reason; изменённый вход даёт conflict; увеличение R учитывает leases, созданные при R=0; смена класса не меняет исходный bucket; после завершений T=B=0. Fake clock использовать для TTL, barriers — для гонок.
- [ ] Запустить `go test ./internal/coordination/local -run PriorityReserve -count=1`, получить ожидаемые FAIL.
- [ ] Добавить счётчик B и атомарные проверки в Acquire под существующим mutex. Обновлять B только при успешном новом admission и в `removeLease` по исходному receipt; не инкрементировать на replay и не декрементировать при отсутствующем lease. Пустой/неизвестный класс отклонять как stale configuration, валидировать R/L до арифметики.
- [ ] Проверить, что reserve refusal оставляет RPM/TPM balances и active lease count без изменений. Запустить `go test -race ./internal/coordination/local ./internal/coordination/contracttest` на платформе с race toolchain.
- [ ] Commit: `feat: enforce priority reserve in local admission`.

### Задача 3: Глобальное атомарное admission в PostgreSQL

**Modify:** `internal/coordination/postgres/admission.go`, `internal/coordination/postgres/replica.go`, `tests/postgres/postgres_test.go` (request fixtures), `tests/postgres/commit_response_loss_test.go`.

**Create:** `tests/postgres/priority_reserve_test.go`. Использовать существующие patterns `admission_parallel_test.go` и общий contract из задачи 2.

**Interfaces:** Coordinator реализует тот же AdmissionRequest/Decision; `request_leases.priority_class` хранит проверенный DB класс на момент выдачи; contract version = 3, rate algorithm version не менять.

- [ ] Подключить общий `PriorityReserve` contract к реальному PostgreSQL. Добавить `TestPostgresPriorityReserveConcurrentReplicas`: два независимых Store/coordinator с разными ReplicaID одновременно держат низкоприоритетные leases, суммарно принято ровно 14 из >=28 попыток; затем оба высоких класса занимают оставшиеся 6; SQL подтверждает T=20/B=14, без дубликатов LeaseID. Не подменять эту проверку локальным mutex.
- [ ] Добавить `TestPostgresPriorityReserveStalePolicyAndClass`: несовпадающие DB/request class или R дают stale configuration без lease/debit; подмена класса не получает резерв. Изменение pool policy сериализовано с admission; изменение client class не меняет уже сохранённый lease. Проверить включение R после low leases при R=0 и поведение при L/R ниже текущей занятости.
- [ ] Добавить `TestPostgresPriorityReserveLeaseLifecycle` и расширить сценарий commit-response-loss: accepted/rejected replay, changed-input conflict, duplicate completion, renewal vs expiry, completion после expiry, прекращение renewal одной реплики без Complete. По DB clock истёкшие leases перестают учитываться до physical cleanup; новый lease можно выдать, старый replay не оживает. Для concurrent high-first сценария контролировать и T, и B.
- [ ] Запустить `go test -count=1 -v -timeout 10m ./tests/postgres -run PriorityReserve` с DSN; зафиксировать ожидаемые FAIL до изменения coordinator.
- [ ] Расширить заблокированное чтение policy полями c.priority_class/p.high_priority_reserve; добавить stale checks. После pool/client locks считать T/B с общим now, проверить total → reserve → client → rates, записать проверенный class в тот же INSERT lease. Сохранять decision reason для replay. Complete/Renew/cleanup используют существующий lease, не вычисляют класс через текущего клиента.
- [ ] Поднять contract version до 3; добавить `TestPostgresPriorityReserveContractCompatibility`: live v2 fingerprint блокирует регистрацию v3, совместимые v3 работают, прежние replay receipts не создают повторную ёмкость. Не добавлять индекс по expires_at: сначала измерить дополнительный aggregate на существующем pool_id index.
- [ ] Запустить новые тесты, существующие admission/replay/cancellation suites и `make test-postgres` с непустым DSN. Сравнить timing существующего parallel admission test до/после на одной БД и отметить дополнительную стоимость count/filter; не заявлять performance SLA без измерения.
- [ ] Commit: `feat: coordinate priority capacity reserve across replicas`.

### Задача 4: Gateway, отказ и телеметрия

**Modify:** `internal/gateway/forward_admission.go`, `internal/gateway/service.go`, `internal/gateway/observer.go`, `deploy/observability/grafana/dashboards/gateway-decisions.json`.

**Tests:** новый `internal/gateway/priority_reserve_test.go`, `internal/httpapi/public_test.go`, `internal/observability/metrics_test.go`, `internal/observability/log_test.go`.

**Interfaces:** Gateway передаёт `PriorityClass` и `PoolHighPriorityReserve` из согласованного authenticated snapshot; `coordinationAPIError(...) *APIError` отображает новый reason в существующий overload envelope. `DecisionPoolPriorityReserve` поступает в обычный RequestEvent.

- [ ] Написать `TestPriorityReserveRequestAndStaleReload`: initial и stale-retry requests содержат актуальные class/R/revision; попытка подменить приоритет через body/header не меняет класс. Запрос после reload получает новый LeaseID, исходный operation receipt не переписывается.
- [ ] Написать `TestPriorityReserveOverloadContract`: low refusal = 429, `error.code == "gateway_overloaded"`, тот же error type/envelope, настроенный Retry-After; DecisionReason = `pool_priority_reserve`; upstream не вызван. При T=L сохранить `pool_inflight_limit`. Проверить reserve refusal не уходит в emergency fallback.
- [ ] Написать `TestPriorityReserveDoesNotBypassSafety` для обоих high классов: client cap, RPM, TPM, MaxWaiting, отсутствие eligible backend и total limit продолжают блокировать. Проверить backend selection failure, отмену и stream close после admission: lease освобождается существующим lifecycle. Для `Admission == nil && R>0` ожидать fail-closed `503 gateway_unavailable`; при R=0 сохранить прежний fallback.
- [ ] Запустить новые targeted tests, получить FAIL; передать поля при каждом Acquire, добавить error mapping и защиту coordinator-free пути. Не создавать отдельного pool lease поверх coordinator lease.
- [ ] Добавить metrics/log assertions с новой причиной и существующими bounded labels. В Grafana allowlist добавить `pool_priority_reserve`; сохранить разбивку по model. Проверить валидный JSON и фактическую query панель.
- [ ] Запустить `go test ./internal/gateway ./internal/httpapi ./internal/observability -count=1`.
- [ ] Commit: `feat: expose priority reserve admission decisions`.

### Задача 5: Admin API и UI

**Modify:** `internal/httpapi/admin.go`, `internal/web/resources.go`, `internal/web/templates/backends.html`, `internal/web/templates/dashboard.html`.

**Tests:** `internal/httpapi/admin_test.go`, `internal/web/web_test.go`; использовать PostgreSQL roundtrip из задачи 1.

**Interfaces:** `PoolInput.HighPriorityReserve int` и `AdminPool.HighPriorityReserve int`, JSON `highPriorityReserve`; структуры PoolInput/Store params сохраняют совместимость существующего явного type conversion. UI input — `high_priority_reserve`, integer >=0; серверная cross-field validation обязательна.

- [ ] Написать `TestAdminPoolPriorityReserveRoundTripAndValidation`: POST/PUT/GET/list/status сохраняют 6 при L=20; отсутствующее поле даёт 0 по существующей full-replacement semantics; reject -1, 21, positive R при L=0 и уменьшение L ниже R. Ошибка не публикует новую registry revision. Успешное изменение доступно gateway после publish/reload.
- [ ] Написать `TestPoolPriorityReserveForms`: create defaults 0, edit prefill 6, POST сохраняет значение; отрицательное/дробное/нечисловое значение не принимается; сервер отвергает неверное сочетание R/L; tables и dashboard показывают резерв. Сохранить auth/CSRF, существующие limits и legacy warning.
- [ ] Запустить `go test ./internal/httpapi ./internal/web -run PriorityReserve -count=1`, получить FAIL.
- [ ] Протянуть поле через input → params → domain → AdminPool; добавить create/edit control и колонки, поправить colspan. Подсказка UI: `Slots reserved for High and Critical. Normal and Background cannot borrow them. 0 disables the reserve; a positive value requires a finite pool inflight limit.`
- [ ] Запустить `go test ./internal/httpapi ./internal/web ./cmd/gateway -count=1`; вручную открыть Backends/dashboard и проверить новую колонку и длинную подсказку на узком экране.
- [ ] Commit: `feat: configure pool priority reserve in admin`.

### Задача 6: Acceptance, аварийный режим и эксплуатация

**Create:** `tests/integration/priority_reserve_test.go`, `tests/postgres/priority_reserve_http_test.go`.

**Modify:** `tests/integration/harness_test.go`, `tests/postgres/http_recovery_performance_test.go`, `docs/operations.md`, `docs/postgresql-production.md`, `docs/acceptance-evidence.md`, `README.md`, `README.ru.md`.

**Interfaces:** Использовать production gateway wiring: SQLite + local coordinator и два HTTP gateway + один PostgreSQL + контролируемый fake upstream. Добавить test helper `createPoolWithReserve(publicName string, limit, reserve int) int64`, не меняя семантику текущего `createPoolWithLimits`.

- [ ] Написать `TestPriorityReserveAdmissionIsolationAcceptance` для SQLite и `TestPostgresPriorityReserveHTTPAdmissionIsolation` для двух gateway. Удерживать 14 потоков Normal/Background (7+7) подтверждёнными barriers; следующий low запрос получает reserve refusal; по 3 High/Critical допущены; любой следующий запрос упирается в total cap 20. Настроить fake metrics, backend capacity, MaxWaiting и клиентские concurrency так, чтобы pressure throttling не маскировал проверяемый reserve boundary.
- [ ] Отменить один low stream, дождаться освобождения lease, допустить ровно один новый low. Закрыть high stream при B=14 — low по-прежнему отклонён, high снова проходит. Затем завершить все потоки и дождаться T=B=0. Отдельный subtest R=0 допускает 20 low streams при прочих равных условиях. Проверить response reason через metrics/log, а не менять публичный overload code.
- [ ] Добавить crash acceptance: остановить renewal одной PostgreSQL-реплики без graceful completion; после TTL её low capacity снова доступна другой реплике, replay старого LeaseID не создаёт новый lease. Подключить commit-loss regression задачи 3 к итоговому прогону.
- [ ] Расширить outage/recovery тест на pool с R>0: Normal/Background получают 503 при недоступной координации; High/Critical ограничены настроенными emergency class/client caps на каждую реплику. R не задаёт emergency cap. После восстановления пройти существующие ping → fingerprint → configuration → leases → failures → circuits barriers до coordinated admission. Активные emergency запросы не объявлять распределёнными leases.
- [ ] Запустить acceptance suites до финальных исправлений и убедиться, что все сценарии действительно выполняются, а не пропускаются из-за DSN; устранить выявленные расхождения реализации с контрактом.
- [ ] Документировать пример `L=20,R=6`, API JSON и PUT semantics, отсутствие borrowing/preemption, взаимодействие с pressure throttling и ограничения GPU/latency. В operations добавить reason и PromQL allowlist. В PostgreSQL guide явно указать: emergency bound = число реплик × class cap для новых emergency admission, плюс уже работающие coordinated запросы; это не гарантия pool reserve/глобального inflight во время outage или перехода восстановления. Нулевой emergency cap запрещает этот класс.
- [ ] Описать rollout: остановить новые admission на всех репликах, drain/завершить активные запросы, остановить v2 процессы, применить миграцию, дождаться истечения старых heartbeat/lease записей при необходимости, запустить только v3 replicas, проверить compatibility/coordination-readyz и затем включать R. Не включать резерв на mixed-version deployment. Для rollback сначала вернуть R=0 и повторить остановку/drain.
- [ ] Выполнить `go test ./...`, `go test -race ./...`, `go vet ./...`, `go build ./cmd/...`, `make test-postgres-docker` на доступной Linux/CI среде. Если используется готовая тестовая БД, вместо Docker wrapper — `make test-postgres` с `LLMGW_POSTGRES_TEST_DSN`. Зафиксировать реально выполненные команды/результаты и ограничения среды в acceptance-evidence; не записывать будущие PASS заранее.
- [ ] Commit: `test: prove priority reserve isolation and document rollout`.

## Финальная проверка реализации

- [ ] Все 8 acceptance criteria issue сопоставлены с выполненными тестами/документацией; строки матрицы выше закрыты.
- [ ] Реальная PostgreSQL-проверка двух реплик выполнена. Успешный `go test ./...` со SKIP PostgreSQL не заменяет эту проверку.
- [ ] Нет второго reservation ID/lease, повторного RPM debit, потери B при class edit и двойного decrement при Complete/expiry.
- [ ] При R=0 сохранены текущие admission решения и public response contract; schema/contract upgrade всё равно требует описанного rollout.
- [ ] Плановые проверки отделены от фактически полученных результатов. Только после этого реализация готова к PR.

## История подготовки и исполнения

2026-09-30: создана отдельная локальная ветка от актуального origin/main; прочитаны issue и соответствующие исходники; план проверен по acceptance criteria. В этом изменении присутствует только документ. Тесты реализации не запускались, поскольку код фичи ещё не написан.


2026-09-30: задачи 1–6 реализованы в `faebaf5`; первоначальные детальные чек-листы выше сохранены как история планирования (имена отдельных тестов и разбиение коммитов при реализации изменились). Домен, обе миграции/store, локальная и PostgreSQL координация, gateway, Admin API/UI, наблюдаемость, HTTP acceptance и эксплуатационные документы готовы. Результаты и ограничения: [acceptance-evidence.md](../../acceptance-evidence.md#priority-capacity-reserve-issue-22--2026-09-30).

Субагент провёл ревью реализации и нашёл Important-гонку устаревшего SQLite snapshot при обновлении резерва/класса, а также Minor-пропуск reason в документированном PromQL. Оба замечания исправлены; повторная проверка этих исправлений тем же ревьюером не нашла новых проблем. Новые barrier-регрессии прошли RED → GREEN; публикация registry snapshot и admission allocation теперь имеют общий порядок под read/write lock. Общие SQLite/PostgreSQL lifecycle contracts и acceptance сохраняют класс уже принятого lease.

| Задача | Фактический статус |
|---|---|
| 1. Модель, validation, миграции и store | Выполнена; roundtrip, ограничения SQL, backfill и guarded downgrade проверены |
| 2. Local admission | Выполнена; subset/total accounting, concurrent acquisition, TTL, replay, completion и policy-update regression проверены |
| 3. PostgreSQL admission | Выполнена; общий лимит двух coordinator/HTTP gateway, stale policy/class, expiry и COMMIT-response loss проверены |
| 4. Gateway и наблюдаемость | Выполнена; overload contract, trusted class, safety guards, metrics/Grafana и stale retry проверены |
| 5. Admin API/UI | Выполнена; API и HTML rendering/form tests прошли; ручная браузерная проверка layout не выполнялась |
| 6. Acceptance и эксплуатация | Выполнена; L=20/R=6, R=0, cancellation, crash/expiry, emergency recovery и rollout задокументированы |

- [x] Все критерии issue сопоставлены с тестами/документацией.
- [x] PostgreSQL acceptance выполнен с реальной БД и двумя gateway.
- [x] Ревью выполнено субагентом, замечания исправлены и повторно проверены.
- [x] Итоговые Windows tests с PostgreSQL, Linux full race и последовательные PostgreSQL race suites, vet и command build прошли.
- [x] Результаты исполнения отделены от исторического плана и ограничений среды.

Решение: для существующих внутренних вызовов coordinator пустой PriorityClass остаётся консервативным lower-классом только при R=0; положительный резерв требует валидный класс. PostgreSQL всегда записывает доверенный класс из БД. Shell-ledger ведётся в PowerShell вместо POSIX helper scripts; это не меняет контракт фичи. Реализация готова к PR после записанной итоговой проверки; push/PR/merge этой задачей не запрашивались.
