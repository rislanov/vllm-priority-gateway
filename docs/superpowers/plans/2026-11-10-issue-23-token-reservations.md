# Request-size-aware Admission — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Реализовать #23: оценка input, server output caps и атомарные outstanding-token reservations с сохранением emergency admission.

**Architecture:** Pure policy/payload layer готовит immutable reservation. Internal estimator использует существующий upstream tokenizer transport; local/PostgreSQL admission хранит резерв вместе с inference lease. Emergency имеет явно локальные token/utility caps и переживает recovery без сброса учёта.

**Tech Stack:** Go 1.27.3, net/http, chi, pgx/PostgreSQL, SQLite, существующие migrations/registry/Prometheus/contracts. Новая tokenizer dependency не требуется.

**Spec:** [design](../specs/2026-11-10-issue-23-token-reservations-design.md).

**Status:** Проект плана; сначала review/согласование спецификации. Product code не изменён. Base `c072e66466d68088c924b94421af3d7c9646d171`, branch `codex/issue-23-token-reservations`.

## Global Constraints

- Emergency High/Critical сохраняется — подтверждено пользователем. Глобальный ceiling при outage/recovery не обещается.
- Disabled/default сохраняет прежние inference/soft-TPM/public-tokenize semantics.
- Token amounts int64, диапазон `0..10^12`, checked sum/multiply. Нет raw content в persistence/logs.
- Три inference endpoints должны иметь явный enabled-policy contract; unsupported input не считается нулём.
- Normal allocation: один receipt + один lease + один RPM debit + один reservation atomically. Retry/expiry/replay не создаёт второй резерв.
- Existing lock order pool → client → lease → receipt и READ COMMITTED post-lock counts сохраняются; network preflight вне locks.
- ModelPool остаётся сравнимым Go struct. ClassLimits — fixed struct, не map.
- vLLM v0.28.0 — qualification baseline. Ни latest renderer, ни новую модель не считать проверенными автоматически.
- История local receipts bounded; PostgreSQL replay window 24h. Сохранять различие guarantees.

## Review Focus

1. Одновременно enabled-policy update, legacy leases и emergency overlap: нельзя обнулить outstanding или незаметно пропустить неоценённый запрос (Tasks 1, 3–6).
2. Tokenizer принимает похожий, но иной prompt (templates, tools, Unicode, Responses history): оценка должна быть доказуемо применима либо явно fallback/reject (Task 2).
3. Same LeaseID с чужим owner, изменённым R или поздним completion: ни чужого release, ни повторного RPM/TPM debit (Tasks 3–4).
4. Recovery во время preflight и ambiguous commit: максимум один generation lifecycle, локальные caps не сбрасываются, permanent fault не маскируется emergency (Tasks 5–6).
5. Unknown usage и telemetry replay: estimated не выдаётся за actual, distributed gauge не суммируется по репликам, содержимое не утекает (Tasks 3–4, 7).

## Карта файлов и интерфейсов

Новые focused modules:

- `internal/domain/request_size.go`: comparable persistent policy/limits, effective resolution.
- `internal/admission/request_size.go`: payload/output validation, checked reservation arithmetic.
- `internal/gateway/token_estimator.go`: bounded preflight orchestration и выбор normal/emergency utility режима.
- `internal/coordination/token_reservation.go`: immutable admission metadata и identity helpers.
- `internal/coordination/contracttest/token_reservation.go`: единая accounting matrix для local и PostgreSQL.
- `internal/observability/token_reservation.go`: numeric-only telemetry.
- Новые test files рядом с каждым module; `tests/postgres/token_reservation_test.go`, `tests/integration/token_reservation_test.go`, `tests/e2e/token_reservation_test.go`.

Существующие integration points: `domain/types.go`, `store/models.go`, `store/clients.go`, `store/pools.go`, `store/snapshot.go`, `store/postgres/configuration.go`, `httpapi/admin.go`, `web/resources.go`, `web/templates/clients.html`, `web/templates/backends.html`, `gateway/forward.go`, `gateway/forward_admission.go`, `gateway/service.go`, `gateway/observer.go`, `coordination/{types.go,emergency.go,lease_manager.go}`, `coordination/local/admission.go`, `coordination/postgres/{admission.go,cleanup.go,replica.go}`, `cmd/gateway/application.go`, `config/config.go`, Makefile/docs.

Нормативные новые типы (реализация тела остаётся исполнителю):

```go
// domain/request_size.go
type RequestSizeLimits struct {
    MaxInputTokens, MaxOutputTokens, DefaultOutputTokens int64
}
type ClassRequestSizeLimits struct {
    Critical, High, Normal, Background RequestSizeLimits
}
type RequestSizePolicy struct {
    Enabled bool
    Limits RequestSizeLimits
    ClassLimits ClassRequestSizeLimits
    MaxOutstandingTokens, EmergencyMaxOutstandingTokens int64
    FallbackMode, EstimationProfile string
    FallbackOverheadTokens int64
}
type EffectiveRequestSizePolicy struct {
    Enabled bool
    Limits RequestSizeLimits
    ClientBudget, PoolBudget, EmergencyPoolBudget int64
    FallbackMode, EstimationProfile string
    FallbackOverheadTokens int64
}
func ResolveRequestSizePolicy(client Client, pool ModelPool) (EffectiveRequestSizePolicy, error)

// admission/request_size.go
type PreparedRequestSize struct {
    Payload []byte
    Inputs []json.RawMessage
    PromptCount, Samples int64
    OutputAllowance int64
}
func PrepareRequestSize(path string, body []byte, policy domain.EffectiveRequestSizePolicy) (PreparedRequestSize, error)
func ReservationTokens(inputEstimates []int64, outputAllowance, samples int64) (int64, error)

// coordination/token_reservation.go
type TokenReservation struct {
    Enabled bool
    InputEstimate, OutputAllowance, PromptCount, Samples, ReservedTokens int64
    ClientBudget, PoolBudget int64
    PoolPolicyRevision int64
    EstimatorKind, EstimationProfile string
}
// AdmissionRequest.TokenReservation stores the immutable value.
// Existing ClientPolicyRevision/ConfigurationRevision remain authoritative.

// gateway/token_estimator.go
type TokenEstimate struct {
    InputTokens []int64
    ContextLimits []int64 // 0 only for explicit fallback without known context
    Kind, Profile string
}
type EstimationMode string // normal | emergency
type InputEstimator interface {
    Estimate(context.Context, EstimationRequest) (TokenEstimate, *APIError)
}
type EstimationRequest struct {
    Path string
    Prepared admission.PreparedRequestSize
    Snapshot *registry.Snapshot
    Client domain.Client
    Pool domain.ModelPool
    APIKeyID int64
    RequestID string
    Mode EstimationMode
}
```

Persistent client fields: `RequestSizeLimits RequestSizeLimits`, `MaxOutstandingTokens int64`; pool field: `RequestSizeAdmission RequestSizePolicy`. Admin JSON uses exact names from spec through explicit DTOs (не shape Go nesting по умолчанию). Internal errors typed/enum; не выводить upstream body в API error.

## Task 1: Policy, persistence и совместимые Admin updates

**Files:** новые `domain/request_size.go`, `domain/request_size_test.go`; менять domain/store/registry/Admin/forms из карты; создать SQLite `006_request_size_admission.sql`, PostgreSQL `000007_request_size_admission.up/down.sql` и migration tests. Проверить свободные номера перед созданием.

**Interfaces:** производит `RequestSizePolicy`, `RequestSizeLimits`, `ResolveRequestSizePolicy`; snapshot несёт policy/revisions; update DTO отличает absent от explicit zero/false. PostgreSQL migration добавляет nullable legacy marker / token metadata к lease/receipt; enabled=false по умолчанию.

- [ ] Добавить `TestResolveRequestSizePolicy`: pool input/output/default=8192/2048/512, Background=2048/256/128, client maxOutput=64 → effective input=2048/output=64/default=64. Client default=32 → default=32. Client=0 наследует; invalid negative/>10^12 rejected.
- [ ] Добавить `TestRequestSizePolicyRoundTripAndLegacyUpdate`: создание old DTO даёт disabled; update только name сохраняет token policy; explicit enabled=false выключает; class/pool/client changes меняют revision; формы не сбрасывают невидимые поля.
- [ ] Добавить migration tests: old schema→new zero defaults, reload equivalent policy, down refuses live nonzero reservation. Использовать native project fixtures; не создавать новый test framework.
- [ ] Запустить `go test ./internal/domain ./internal/store/... ./internal/httpapi ./internal/web -run RequestSize -count=1`, убедиться в ожидаемом failure новых контрактов.
- [ ] Реализовать policy/validation, SQLite/PostgreSQL CRUD/snapshot и Admin/form persistence. Добавить cross-entity validation/error path. Новые JSON поля обновлять pointer/presence-aware DTO; не превращать unrelated API validation в refactor.
- [ ] Повторить tests, отдельно выполнить migration tests с disposable PostgreSQL. Commit: `feat: persist opt-in request size admission policies`.

## Task 2: Payload bounds и bounded token estimation

**Files:** новые `admission/request_size.go`, `admission/request_size_test.go`, `gateway/token_estimator.go`, `gateway/token_estimator_test.go`; менять `proxy/tokenization.go` только для bounded internal decoder adapter, `config/config.go`, новый `config/request_size.go`; тесты рядом.

**Interfaces:** потребляет Task 1 policy; производит PreparedRequestSize, TokenEstimate, InputEstimator. Transport остаётся `Proxy.Tokenize(ctx, proxy.TokenizationRequest) (*http.Response,error)`. Normal preflight получает utility lease через existing TokenizationCoordinator; emergency acquisition dependency внедряется в Task 5.

- [ ] `TestPrepareRequestSizeEndpoints`: отсутствующий cap →512 (либо effective default); explicit 2049 при max2048 →400; chat aliases 128/128 нормализуются, 128/64 rejected; response max_output_tokens сохраняется; min_tokens>cap rejected; streaming same cap.
- [ ] `TestReservationTokensMultipliers`: inputs=[100,200], output=50,n=2 →800; inputs=[0],output=50,n=1 →51; n=0/>32, batch>32, invalid mixed batch, overflow и amount>10^12 rejected.
- [ ] `TestEstimatorCompatibility`: все строки нормативной field matrix spec §5 имеют отдельные fixtures; Unicode, BOS/special tokens, generation prompt и continue_final_message materialize одинаково; tools/schema/inline function history используют explicit fallback/reject. Неизвестный top-level field rejected; token-ID prompt проверяется как единый prompt. Для unsupported Responses fallback=reject →400; utf8_bytes →byteLength(canonicalJSON)+overhead, никогда 0.
- [ ] `TestEstimatorBoundsAndPrivacy`: 2s total deadline across batch, max response 8 MiB, cancellation, noninteger/missing count, count/tokens mismatch, context overflow, 401/403, 404/501, 429/5xx; fallback только для unsupported, не для temporary failures. Ни utility retries, ни inference counters, ни body в ошибке.
- [ ] Запустить `go test ./internal/admission ./internal/gateway ./internal/proxy ./internal/config -run 'RequestSize|ReservationTokens|Estimator' -count=1`; зафиксировать expected failures.
- [ ] Реализовать pure validation и estimator, включая table/allowlist input-bearing fields. Test fixtures закрепляют v0.28.0 mapping; неоднозначные варианты проходят fallback/reject, не эвристику «только content».
- [ ] Проверить PASS и `go test ./internal/gateway ./internal/proxy -run Tokenization -count=1`. Commit: `feat: estimate request tokens and enforce output allowances`.

## Task 3: Local atomic reservation и безопасный owner lifecycle

**Files:** новые `coordination/token_reservation.go`, `coordination/contracttest/token_reservation.go`, `coordination/local/token_reservation_test.go`; менять `coordination/types.go`, `coordination/local/admission.go`, `coordination/local/contract_test.go`.

**Interfaces:** `AdmissionRequest.TokenReservation`; `TokenReservationContract(t *testing.T, factory TokenReservationFactory)` с factory, которая создаёт isolated coordinator/configuration, умеет Advance/SetPolicy/Outstanding. Не привязывать assertions к storage clock. LeaseCompletion использует текущий Usage; metadata берётся из receipt.

- [ ] В общем contract закрепить Bpool=1000/Bclient=800/R=600/400/1 example из AC05; concurrency/RPM failure не расходует tokens; token failure не расходует RPM; same Acquire 100 раз выделяет R один раз.
- [ ] `TestTokenReservationLifecycle`: valid actual=250 → release600 и один TPM debit250; unknown usage → release600, actual unknown; expiry→release600; late actual→TPM один раз, outstanding unchanged; Complete replay→release0.
- [ ] `TestTokenReservationOwnerMismatch`: менять по одному ClientID/PoolID/ReplicaID/RequestID в Renew и Complete; чужой operation не меняет lease, expiry, counters или TPM. Valid owner после этого продолжает работать. Показать existing Renew defect failing regression до fix.
- [ ] `TestTokenReservationPublicationAndReplayEviction`: policy decrease не переписывает R; disable pool не убирает старые token leases из client sums; enable with legacy leases rejected until drain; evicted receipt original timestamp не resurrect; existing live lease проверяется до cutoff. Future-start token receipt не evict до now≥start, all-protected full capacity даёт unavailable без future cutoff; valid текущие owners могут завершиться.
- [ ] Запустить `go test ./internal/coordination/local -run TokenReservation -count=1`, ожидать failures; затем добавить counters в общую mutex section и снимать только stored R через removeLease. Добавить безопасный replay cutoff для enabled admissions и immutable fingerprint metadata.
- [ ] `go test -race ./internal/coordination/local ./internal/coordination/contracttest` →PASS. Commit: `feat: reserve outstanding tokens in local admission`.

## Task 4: PostgreSQL shared reservation и recovery contracts

**Files:** менять `coordination/postgres/{admission.go,cleanup.go,replica.go}`, при необходимости migration из Task1; новые `tests/postgres/token_reservation_test.go`; расширить `tests/postgres/commit_response_loss_test.go`, `cleanup_concurrency_test.go`, `configuration_delete_test.go`, `admission_parallel_test.go` и startup compatibility tests.

**Interfaces:** тот же AdmissionCoordinator/TokenReservationContract; PG authoritative budget из persisted policy; normal shared outstanding=sum unexpired reserved_tokens. ContractVersion 4, RateAlgorithmVersion остаётся 1.

- [ ] Запустить общий Task3 contract для PostgreSQL. Добавить 2 coordinator instances: Bpool=1000, 100 concurrent acquisitions R=100, свободные concurrency/RPM →ровно10 admits, peak1000. Разные pools одного клиента делят client budget.
- [ ] `TestPostgresTokenReservationLostCommit`: drop successful Acquire commit response →same lease/RPM/R once; changed R/profile →idempotency conflict. Complete commit loss и partial batch retry не double-release/double-TPM.
- [ ] `TestPostgresTokenReservationExpiryAndPolicyRace`: counts исключают expiry до cleanup; cleanup/Complete/Renew races; config delete/tombstone не мешают terminal release; policy decrease, client class change, enable transition и wrong-owner операции не обходят limits.
- [ ] Выполнить `make test-postgres` на disposable DSN и подтвердить, что новые tests реально executed (не skip).
- [ ] Реализовать atomic check/sums/write на existing pool→client lock order, authoritative policy validation, fingerprint и receipt metadata. Owner matching сравнивает immutable receipt identity. SUM(bigint) обрабатывать без unchecked int overflow. Сохранять pending-prefix results и synchronous_commit.
- [ ] Проверить compatibility/drain upgrade 3→4 и rollback refusal. Измерить admission contention latency существующим test profile, не держать SQL locks во время tokenizer call.
- [ ] `make test-postgres` →PASS; `go test -race ./internal/coordination/...` →PASS. Commit: `feat: coordinate token reservations across gateway replicas`.

## Task 5: Emergency tokens и независимый preflight при outage

**Files:** менять `coordination/emergency.go`, добавить `coordination/emergency_tokens_test.go`; дописать estimator emergency provider/config, `cmd/gateway/application.go`; `gateway/service_pool_test.go` и application recovery tests.

**Interfaces:** сохранить existing Acquire для disabled flow; добавить `EmergencyAdmission.AcquireTokens(class domain.PriorityClass, clientID, poolID int64, clientLimit int, reservation TokenReservation, emergencyPoolBudget int64) (func(), bool)`. Release closure покрывает concurrency и tokens одним once. Emergency estimator использует отдельный local TokenizationCoordinator c собственными лимитами; public utility coordinator не подменять.

- [ ] `TestEmergencyTokenCaps`: High/Critical eligible, Normal/Background denied; Bpool=1000, clientBudget800, R600 →second same-client denied; другой client R400→admit; release/replay balanced; existing class caps остаются ограничивающими.
- [ ] `TestEmergencyEstimatorWithoutDatabase`: unavailable PG, healthy tokenizer →High запрос получает bounded estimate и локальный reservation. Public `/tokenize` по-прежнему503. Concurrent emergency preflights ограничены 1/client,1/backend,30 RPM/burst1 defaults. Override config покрыт fingerprint tests.
- [ ] `TestEmergencyEstimatorTransitionMidBatch`: первый prompt оценён, normal utility Acquire второго теряет DB →один переход в local mode с новым utility ID и оставшимся общим deadline; estimate первого сохранён только с прежним profile. Utility429/permanent fault/upstream timeout не запускают fallback. Recovery не переключает этот request обратно в normal.
- [ ] `TestEmergencyTokenRecoveryOverlap`: normal readiness вернулась при live emergency request; локальный резерв остаётся до completion. Повторный outage не обнуляет counters; permanent error не переключается на emergency. Invalid/missing estimator не допускает unbounded запрос.
- [ ] Запустить `go test ./internal/coordination ./internal/gateway ./internal/config ./cmd/gateway -run 'Emergency.*Token|EmergencyEstimator|TokenRecovery' -count=1`, подтвердить failures.
- [ ] Реализовать shared local critical section, provider selection, lifetime/release и recovery overlap, сохранив семантику disabled pool. Normal tokenization и emergency tokenization budgets учитывать отдельно в telemetry.
- [ ] Повторить targeted tests и `go test -race ./internal/coordination ./internal/gateway`; PASS. Commit: `feat: preserve bounded emergency token admission`.

## Task 6: Подключить preflight к inference lifecycle

**Files:** `gateway/forward.go`, `gateway/forward_admission.go`, `gateway/service.go`, `gateway/service_retry_test.go`, `gateway/usage_test.go`, `httpapi/public_test.go`; новые `tests/integration/token_reservation_test.go`, `tests/postgres/token_reservation_http_test.go`; wiring application.

**Interfaces:** `resolvedForwardRequest` хранит PreparedRequestSize/TokenEstimate/TokenReservation и immutable admission mode. `acquireForwardAdmission` сохраняет release callback signature; estimator вызывается до allocation. Пересчёт при stale config использует исходный request.Body, не уже переписанный payload.

- [ ] `TestTokenAdmissionHTTPMatrix`: все endpoints/streaming, no-tokenizer when disabled, budget429+Retry-After, invalid size400 без generation/RPM, preflight503, route eligibility/affinity и limits выше priority.
- [ ] `TestTokenAdmissionPolicyChangedDuringEstimate`: revoke/disabled model/profile сменился во время blocked estimate →no dispatch; size policy change →один re-estimate; повторный change →503. Enable gate учитывает legacy distributed leases и локально выполняющиеся legacy emergency requests; deployment docs требуют cluster-wide drain для emergency overlap.
- [ ] `TestTokenAdmissionRetryAndCancellation`: pre-byte retry→один reservation, post-byte retry запрещён; cancel после acquisition, upstream error и panic освобождают once; stream>TTL renews R; backlog overflow освобождает через expiry.
- [ ] `TestTokenAdmissionAmbiguousFallback`: unresolved normal Acquire + valid emergency →одна generation, возможный ghost distributed reservation истекает; normal replay не меняет UUID; completion emergency не мутирует неизвестный normal lease.
- [ ] Запустить HTTP/integration tests, получить ожидаемые failures; подключить Tasks1–5, добавить причины в coordinationAPIError и bounded decision enum.
- [ ] Проверить `go test ./internal/gateway ./internal/httpapi ./tests/integration -count=1`, `make test-postgres`, `go test -race ./internal/gateway ./internal/coordination/...`. Commit: `feat: enforce request size admission across inference lifecycle`.

## Task 7: Наблюдаемость, UI и эксплуатационные инструкции

**Files:** `gateway/observer.go`, новый `observability/token_reservation.go`/test; `observability/metrics.go`, `observability/log.go`, coordinator observer hooks; Admin templates/tests, `docs/operations.md`, `docs/postgresql-production.md`, README.md/README.ru.md.

**Interfaces:** добавить optional `TokenReservationObserver` с `TokenReservationChanged(TokenReservationEvent)`; event — pool/client IDs, class, mode, action (acquired/released/expired/reconciled), amounts, usage-known flag, bounded reason. Для Postgres events не обещать exactly-once delivery: metrics monotonic events best effort, authoritative outstanding snapshot источник истины. RequestEvent получает numeric estimate/reserve и estimator/mode.

- [ ] `TestTokenReservationTelemetry`: reserve600/actual250/unknown — три разных состояния; replay не генерирует повторный local accounting callback; expired release виден без HTTP completion. PG uncertain callback delivery не влияет на correctness.
- [ ] `TestTokenReservationPrivacy`: уникальные sentinel prompt/generated/tokenizer strings не встречаются в persisted receipts, structured events/logs, metrics labels или API errors. Пустой actual не сериализуется как фактический0.
- [ ] `TestRequestSizeAdminForms`: effective limits, clear disabled/inherited labels и emergency per-replica copy; form partial update не сбрасывает policy; CSRF/auth сохраняются. Проверить текущий template test workflow.
- [ ] Реализовать bounded metrics/observer и UI. Документировать utility preflight capacity planning, fallback calibration, disabled pools/client-budget boundary, High headroom limitation, unknown usage, enable drain и recovery overlap.
- [ ] `go test ./internal/observability ./internal/httpapi ./internal/web -count=1` и `make test-web-unit`; PASS. Commit: `feat: expose token reservation decisions and operating limits`.

## Task 8: Qualification, real-vLLM experiment и итоговая приёмка

**Files:** `tests/e2e/token_reservation_test.go`, `tests/e2e/harness_test.go`, `tests/e2e/harness_unit_test.go`, `Makefile`, `docs/real-vllm-priority-e2e.md`, `docs/issue-23-analysis-and-evidence.md`.

**Interfaces:** новый e2e mode `tokens`; `make test-real-vllm` принимает его. Использовать существующие gateway/model/admin/key env names; новые `LLMGW_E2E_TOKEN_RUNS=3`, `LLMGW_E2E_TOKEN_HIGH_PROBES=100` и воспроизводимый seed. Печать numeric experiment report без prompt content.

- [ ] Unit tests конфигурации/сценария: invalid mode/missing credentials объясняются, counters различают attempts/admissions/rejections, TTFT missing не подменяется0; сценарий fail если не достиг token-pressure или не набрал100 probes/повтор.
- [ ] Реализовать paired disabled/enabled 3× эксперимент из spec §11 с warm-up, проверкой no collateral policy edits, cleanup/restore test-owned settings, caps/unknown/error accounting и ratio/percentiles. Disabled и enabled запускать на одном workload/model config, фиксируя порядок.
- [ ] Выполнить `go test -count=1 ./...`, `go test -race ./...`, `go vet ./...`, `go build ./cmd/...`, `make test-web-unit`, `make test-postgres`; записать commands, commit, env/profile, pass/fail/skips. Для PostgreSQL обязательны реальные two-replica executions.
- [ ] Выполнить `LLMGW_E2E_MODE=tokens make test-real-vllm` на выделенном real-vLLM target с подтверждённым model profile и sane secrets handling. Прогнать direct tokenizer-vs-generation qualification на всех backend для поддерживаемых точных payloads. Без окружения отметить AC14 pending, не считать synthetic tests заменой.
- [ ] Заполнить AC01–AC14 evidence со ссылками на tests и результатами. Подтверждённые дефекты устранить с regression либо явно согласовать исключение. Выполнить whole-branch review перед PR.
- [ ] Commit: `test: qualify token reservations under mixed inference load`. Публикация PR/закрытие issue — после готового implementation/evidence, не на стадии этого проекта плана.

## Самопроверка плана и порядок выполнения

Зависимости: 1→2/3; 3→4; 2+3→5; 1–5→6; 6→7→8. Пока выполнять последовательно: policy/fingerprint/lifecycle contracts общие, преждевременный parallel code editing увеличивает integration риск. Отдельный builder review спецификации выполнен: уточнены field matrix, normal→emergency preflight, cluster-wide drain, disabled pool old reservations и bounded replay cutoff; соответствующие tests добавлены.

Матрица покрытия: AC01=6; AC02=1,7; AC03–04=2,6,8; AC05=3–4; AC06=1,3–4,6; AC07–09=3–4,6; AC10=5–6; AC11=2,5; AC12=7; AC13=1,4; AC14=8. Review Focus включён в named tests выше. Этапы заканчиваются собственным проверяемым deliverable, итоговые checks не заменяют их.

Перед реализацией согласовать конкретную спецификацию, особенно opt-in ограничения Responses/media, shared utility preflight budgets, отсутствие token-priority reserve и сохранение soft-TPM unknown-usage semantics. Emergency уже является обязательным требованием пользователя, повторно его не согласовывать.
