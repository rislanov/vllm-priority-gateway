# Issue #23 — анализ и журнал доказательств

Дата планирования: 2026-11-10 (Europe/Budapest).
Issue: https://github.com/rislanov/vllm-priority-gateway/issues/23
Base commit: `c072e66466d68088c924b94421af3d7c9646d171`.
Branch: `codex/issue-23-token-reservations`, создана от обновлённого origin/main.

## Результат предварительного анализа

Это изменение admission protocol, затрагивающее policy storage, API/UI, preflight, два coordinator backend, streaming lifecycle, emergency и доказательство на vLLM. Одного middleware перед HTTP proxy недостаточно.

Уже реализовано и переиспользуется:

- #22: slot reserve и проверки total/lower-priority inflight. Он не защищает токеновую ёмкость.
- #33 (closed): authenticated `/tokenize`, отдельный utility coordinator и общий `Proxy.Tokenize`. Public utility нельзя использовать как вложенный inference lifecycle.
- Admission receipt/fingerprint, lease renewal/expiry, partial completion retry, PostgreSQL lost-commit tests.
- Inference usage inspection и soft-TPM charge по фактическому usage; bounded completion queue.
- PostgreSQL startup compatibility/recovery barriers и graceful unregister.

Критические риски и решения:

| Риск | Предлагаемая мера |
| --- | --- |
| Проверить token budget до SQL transaction и затем отдельно allocate | Проверка sums, lease/RPM/reservation в одной существующей admission transaction |
| Дубли после timeout/потерянного commit response | Immutable reservation в том же fingerprint/receipt; normal retry того же operation ID |
| Двойное освобождение на expiry+Complete | Stored R и единый terminal/remove path; late actual отдельный TPM debit once |
| Чужой Renew удаляет local lease | Подтверждённый source lead; обязательный regression/fix до counters, связан с #43 |
| Смена policy во время токенизации | Revalidation/re-estimation, bounded retry; старый payload не dispatch при смене model/profile |
| Включение budget при legacy active requests | Enable drain gate; cluster-wide drain при legacy emergency overlap |
| `/tokenize` и inference форматируют input по-разному | Pinned model/template profile, explicit compatibility fixtures/fallback; Responses не преобразовывать в chat наугад |
| Estimation перегружает tokenizer | Shared normal utility quotas, bounded deadline/body, отдельные emergency local quotas |
| Случайный отказ от emergency | Пользователь явно требует сохранить; local token caps и независимый emergency preflight |
| Normal+emergency overlap после recovery | Сохранять local lifecycle до завершения; честно документировать отсутствие global bound |
| Неизвестный usage выглядит как нулевой | Отдельный unknown; оценки не вставлять в actual analytics/TPM |
| Lower priority заполняет pool token budget | Не обещать priority token isolation; ограничить size/client budgets, измерить real-vLLM эффект; отдельного token reserve в scope нет |

## Что реально проверено при планировании

Environment: macOS/darwin arm64, `go version go1.27.3 darwin/arm64`.

| Проверка | Результат |
| --- | --- |
| `git fetch origin`, issue #23 и #33 через GitHub connector | #23 open, assignee rislanov; #33 closed/completed |
| `go test ./internal/admission ./internal/coordination/... ./internal/gateway ./internal/proxy ./internal/httpapi ./tests/integration` | PASS (exit0), исходная реализация |
| `go test -count=1 ./...` | PASS (exit0), исходная реализация; не доказательство новой feature |
| Live PostgreSQL integration | Не выполнялось: `LLMGW_POSTGRES_TEST_DSN` не задан; conditional tests skip |
| Real vLLM / GPU / TTFT | Не выполнялось: e2e mode/backend не настроены; conditional tests skip |
| Race/vet/browser qualification новой feature | Не выполнялось: реализации пока нет |

Полный baseline output при планировании: `/tmp/issue-23-baseline-tests.log` (временный локальный файл, не долговременный артефакт). После реализации evidence должен ссылаться на новый tested commit и сохранённые обезличенные результаты, а не на этот baseline.

Codebase graph использован в Tier 2: index_status перед discovery, search_graph, Forward/Complete traces и exact snippets. Покрытие evidence paths проверено через check_index_coverage (последняя проверка при написании spec: generation `2026-11-10T22:38:41Z`, no_recorded_issue/metadata_match). Граф обновляется автоматически. Это best-effort signal, не доказательство отсутствия дефектов. Broad symbol searches с неполными страницами использовались только для положительного поиска; exhaustive conclusions не делались. Source reads подтвердили материальные выводы и неоднозначные same-name Go method traces.

Upstream schemas проверены по pinned v0.28.0 source, ссылки в [spec](superpowers/specs/2026-11-10-issue-23-token-reservations-design.md). Это проверка API shape, не проверка live модели/чата/всех template options.

## Приёмка реализации

Builder review спецификации выполнен отдельным агентом по brainstorming skill. Его пять существенных вопросов закрыты в spec/plan: точные input fields для exact/fallback; переход в emergency при отказе normal preflight; обязательный cluster-wide drain legacy emergency перед enable; сохранение old reservations в sums после disable; replay cutoff без продвижения в будущее при receipt eviction. Это review документа, не code/security review реализации.

Все AC01–AC14 сейчас **pending**: baseline tests не доказывают ещё не реализованную feature. Заполнять таблицу после соответствующего этапа.

| Критерии | Будущий evidence artifact | Статус |
| --- | --- | --- |
| AC01–02 | Policy/persistence/Admin/disabled contract results | pending |
| AC03–04, AC11 | Payload/estimator endpoint and boundedness fixtures | pending |
| AC05–09 | Local/PG shared contracts, concurrency, lost-commit/expiry tests | pending |
| AC10 | Emergency outage/recovery two-gateway evidence | pending |
| AC12 | Metrics/privacy and numeric unknown/actual assertions | pending |
| AC13 | Migrations, compatibility version/drain/rollback evidence | pending |
| AC14 | Real-vLLM paired experiment with hardware/model/revisions/config | pending |

Документы: [спецификация и критерии](superpowers/specs/2026-11-10-issue-23-token-reservations-design.md), [пошаговый план](superpowers/plans/2026-11-10-issue-23-token-reservations.md).
