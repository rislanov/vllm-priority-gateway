# Issue #23: request-size-aware admission — спецификация

Статус: проект для согласования, реализация не начата. Дата: 2026-11-10.
Issue: https://github.com/rislanov/vllm-priority-gateway/issues/23
Ветка: `codex/issue-23-token-reservations`.
База: `c072e66466d68088c924b94421af3d7c9646d171` (`origin/main`).

Пользователь подтвердил #23 и потребовал **сохранить emergency-допуск**. Остальные решения ниже — предложения автора спецификации, а не ранее согласованные требования пользователя.

## 1. Задача и границы

Не допускать одновременно слишком много больших запросов, пока их фактическое потребление ещё не появилось в soft TPM. Перед генерацией оценивать input, ограничивать output и атомарно резервировать `R` токенов на весь inference lifecycle. Применять ограничения по модели, классу и клиенту. PostgreSQL координирует реплики; SQLite остаётся single-replica с in-memory coordination.

Существующие client concurrency, pool inflight, High/Critical slot reserve, RPM, soft TPM, waiting limits, backend eligibility и circuit protection продолжают работать. Caller-supplied priority и оценки токенов не являются доверенными.

Не обещаем GPU/KV allocation, preemption, exactly-once inference, фиксированный TTFT или глобальную изоляцию при отказе PostgreSQL. Не добавляем очередь, локальную библиотеку tokenizer, хранилище промптов, новую систему биллинга или переработку аналитики. Публичный `/tokenize` сохраняет отдельный lifecycle и прежнее fail-closed поведение.

## 2. Рассмотренные подходы

| Подход | Преимущество | Цена / решение |
| --- | --- | --- |
| Отдельный outstanding-token budget, soft TPM по actual usage | Совместимость с существующими квотами; один lease на запрос | Рекомендуется. Это ограничение незавершённой работы, не резерв RPM/TPM |
| Предоплата из TPM с возвратом разницы | Заранее ограничивает минутное потребление | Меняет действующий контракт, требует политики debt/refund при refill, смене revision и неизвестном usage; отдельное расширение |
| Только лимиты input/output | Простая интеграция | Не ограничивает сумму одновременно допущенных запросов; недостаточно для #23 |

Emergency сохраняется через отдельные per-replica token caps. Отказ от emergency ради глобального бюджета пользователь отверг.

## 3. Текущее устройство и точки изменения

1. `internal/gateway/forward.go`: auth/snapshot → response-completion reservation → повторная auth → prepare payload → pool gate → admission → backend selection → proxy → deferred completion. Сетевую оценку нельзя выполнять под registry lock или SQL transaction.
2. `internal/gateway/forward_admission.go`: строит `AdmissionRequest`, один раз перезагружает stale config, при временном отказе координации использует `EmergencyAdmission`. После перезагрузки потребуется пересчитать payload и estimate.
3. `internal/coordination/local/admission.go`: mutex, leases, receipts, client/pool counters; RPM списывается при admission, TPM — по валидному usage в `Complete`. Nil usage сейчас не списывает TPM.
4. `internal/coordination/postgres/admission.go`: receipt и fingerprint, pool → client locks, fresh READ COMMITTED counts, lease и RPM в одной durable transaction. Повтор после ambiguous commit использует тот же LeaseID. Completion сериализован теми же scopes.
5. `internal/coordination/lease_manager.go`: renewal, bounded completion delivery, partial-prefix retry. При переполнении completion backlog прекращает tracking; освобождение обеспечивается TTL, actual TPM может не дойти.
6. `internal/proxy/tokenization.go`: `Proxy.Tokenize` уже предоставляет общий upstream adapter. `Service.Tokenize` является публичным utility lifecycle; вызывать его рекурсивно из inference нельзя.
7. Utility tokenizer имеет отдельные client/backend concurrency и RPM, общий PostgreSQL budget; defaults 4 / 8 / 120 RPM, timeout 30s. Внутренний preflight должен учитывать его нагрузку.
8. `internal/coordination/postgres/replica.go`: coordination contract version сейчас 3; изменение persistent admission semantics требует нового contract version и upgrade evidence.

Наблюдение для обязательной проверки: local `Renew` вызывает `removeLease` при несовпадении ClientID/PoolID. До добавления token counters нужен regression test, доказывающий, что чужой identity не освобождает чужой резерв. Это также связано с #43; полный lifecycle QA не входит в #23.

## 4. Конфигурационный контракт

Все token amounts — `int64`, допустимый диапазон `0..10^12`; арифметика checked, никакого float для reservation. JSON field names ниже нормативны. Новые поля отсутствуют/равны нулю в мигрированных конфигурациях.

### Pool: объект `requestSizeAdmission`

| Поле | Значение / правило |
| --- | --- |
| `enabled` | false по умолчанию; master switch для данного pool |
| `maxOutstandingTokens` | >0 при enabled; общий pool budget |
| `maxInputTokens`, `maxOutputTokens`, `defaultOutputTokens` | >0 при enabled; default ≤ maxOutput |
| `classLimits` | Фиксированные поля critical/high/normal/background с maxInputTokens/maxOutputTokens/defaultOutputTokens; 0 означает наследовать |
| `fallbackMode` | `reject` по умолчанию; опционально `utf8_bytes` для self-contained text |
| `fallbackOverheadTokens` | >0 для utf8_bytes; оператор калибрует model/template overhead |
| `estimationProfile` | Непустой идентификатор проверенного tokenizer/template/model deployment при enabled; не автоматически полученное доказательство идентичности |
| `emergencyMaxOutstandingTokens` | 0 означает наследовать maxOutstandingTokens **на каждой реплике**; положительное значение ≤ maxOutstandingTokens |

Ограничения клиента: `maxInputTokens`, `maxOutputTokens`, `defaultOutputTokens`, `maxOutstandingTokens`. Все по умолчанию 0. Первые три ограничивают запрос в enabled pool, последнее — общий клиентский outstanding budget по всем enabled pools и ключам клиента. Отключённые pools не включаются в новый token accounting; UI обязан явно показывать эту границу.

Effective input/output maximum = минимум положительных pool, class и client значений. Default: client, иначе class, иначе pool; итоговый default = min(выбранный default, effective output maximum). Explicit output выше maximum — ошибка, не молчаливое обрезание. Обновление pool/class/client проверяет диапазоны и relationships; если cross-entity изменение временно делает policy несовместимой, admission отклоняется как invalid/stale policy, а не снимает лимиты.

Рекомендуемая конфигурация, **не product default**: pool input 8192, output 2048, default 512, outstanding 32768; Background input 2048/output 256/default 128, client outstanding 8192. Числа надо калибровать на целевой модели.

При `enabled=false` нет preflight, новых output rewrites и token reservations; действует прежний admission, включая emergency и soft TPM. Смена false→true на работающем pool требует drain ранее допущенных legacy leases: пока есть unexpired lease без token accounting, новые enabled admissions получают retryable 503 `token_policy_transition`. Новое class/client ограничение существующие token leases не пересчитывает. Снижение budget ниже outstanding запрещает новые admission до освобождения; старые запросы не прерываются. true→false не обнуляет уже выданные reservations: они завершаются по сохранённому lease contract. Такие старые token leases продолжают входить в pool/client sums независимо от текущего enabled; исключаются лишь новые legacy leases, выданные после выключения.

Для false→true оператор обязан остановить admission и дренировать **все реплики**, в том числе legacy emergency streams, затем включить policy и возобновить traffic. Shared lease gate — дополнительная защита, не распределённое доказательство завершения emergency: PG их не видит. На своей реплике gateway отслеживает legacy emergency lifecycles по pool и возвращает `token_policy_transition` также из emergency path до их завершения. Этот отказ не переключается обратно в legacy mode. Включать политику во время продолжающейся аварии без cluster-wide drain не поддерживается.

## 5. Endpoint и payload contract

Поддержка ниже относится к **enabled** pool. Disabled сохраняет существующий pass-through.

| Endpoint | Output limit | Input estimate |
| --- | --- | --- |
| `/v1/chat/completions` | `max_completion_tokens`; legacy `max_tokens` принимается как alias | `/tokenize` с messages и проверенными template options |
| `/v1/completions` | `max_tokens` | `/tokenize` для string prompt; token-ID input — проверенная длина; batch — сумма отдельных элементов |
| `/v1/responses` | `max_output_tokens` | Явный `utf8_bytes` fallback для self-contained text; при fallback=reject — 400 `token_estimation_unsupported` |

Нет output limit / null → подставить effective default. Числа должны быть положительными целыми, без coercion из string/bool/float. Для chat два ненулевых alias с разными значениями → 400; одинаковые нормализовать в `max_completion_tokens`, удалить `max_tokens`. Wrong-endpoint output alias и конфликтующий `min_tokens` → 400. Backend получает ровно тот cap, который участвовал в резервировании. Streaming использует те же правила.

Для completions разрешены string, непустой token-ID array, непустой homogeneous array строк или token-ID arrays. ID — неотрицательные int64. Не считать token-ID array как batch. Максимум 32 prompts и `n` от 1 до 32; checked multiplication. `n=null` трактуется как 1 только там, где upstream это поддерживает. Для каждого prompt input limit проверяется отдельно, aggregate input — сумма. Для chat один prompt. Для Responses n не поддерживается.

Консервативная формула: `R = n × (sum(inputEstimate[j]) + promptCount × effectiveOutputAllowance)`. Она намеренно учитывает input для каждой генерации, даже если upstream умеет sharing. Input tokens после prefix-cache hit не вычитаются. `best_of`, beam search и неизвестные generation multipliers в v1 rejected, чтобы не резервировать меньше фактического fan-out. Пустой prompt может иметь фактический нулевой count, но accounting minimum каждого prompt = 1; отличать это от ошибки estimator.

В v1 enabled-policy отклоняет media/remote files, `previous_response_id`, server-side prompt references, Responses `background=true` и server-side tool execution: полный input или срок generation неизвестен шлюзу. Self-contained text и inline function-call history допустимы лишь через проверенный adapter/fallback. Внешние tools не вызываются шлюзом. Отказы — документированный opt-in compatibility boundary, не скрытый обход лимита. Новые варианты payload расширяются отдельными contract fixtures.

`truncate_prompt_tokens`, Responses truncation=auto и другие input truncation overrides в enabled v1 rejected: admission не должен считать один prompt, а отправлять другой. Передача клиентского chat template допускается только если tokenizer adapter передаёт идентичные options; недоказанное соответствие, включая model-specific tool/response-format rendering, переводится в explicit fallback либо reject.

### Нормативная матрица input fields v1

| Поля / форма | Exact adapter | Fallback canonical input |
| --- | --- | --- |
| Chat `messages` с role system/developer/user/assistant, content string либо text-only parts | Передать messages без изменения; без tool/function history | Весь messages, включая роли и text parts |
| Chat `add_generation_prompt`, `continue_final_message`, `add_special_tokens`, `chat_template`, `chat_template_kwargs` | Передать все присутствующие поля; absent materialize defaults true/false/false для первых трёх; если continue_final_message=true и generation flag absent, materialize generation=false в обоих запросах | Все эти поля с теми же effective defaults |
| Chat tools/tool_choice, response_format, function_call/functions; messages с tool_calls/tool role/function role/reasoning fields | В v1 не exact: explicit fallback или reject | Полные структуры вместе с messages; не вынимать только arguments/content |
| Completions prompt string | Передать prompt и effective add_special_tokens (default=true) | Объект prompt+effective add_special_tokens |
| Completions token-ID prompt | Count validated IDs, не вызывать tokenizer; IDs не дополняются специальными tokens | Fallback не нужен |
| Completions batch | Обработать каждый prompt по предыдущим двум строкам | По одному canonical input на prompt |
| Responses input string / inline message items с input_text/output_text; inline function_call/function_call_output | Нет direct `/tokenize` mapping | Полные input, instructions, tools (только type=function), tool_choice, text, reasoning, chat_template_kwargs |
| Responses previous_response_id, prompt reference, previous_input_messages, background=true, non-function tools | Reject | Reject |
| Media content, URL/file references, prompt_embeds, mm_processor_kwargs/media_io_kwargs, multimodal или скрытый history | Reject | Reject |

Exact chat qualification остаётся model-profile-specific: даже совпадающие поля не доказывают совпадения renderer; профиль должен подтвердить count относительно generation usage на fixtures. Completions token-ID форма требует backend profile, где generation не добавляет к supplied IDs скрытые tokens; иначе reject.

Top-level fields, которые v1 разрешает как sampling/response-only и исключает из canonical input: model (записывается в profile), stream, stream_options, output aliases, n, min_tokens, temperature, top_p, top_k, min_p, seed, stop, stop_token_ids, frequency_penalty, presence_penalty, repetition_penalty, ignore_eos, logprobs, top_logprobs, logit_bias, echo, skip_special_tokens, spaces_between_special_tokens, include_stop_str_in_output, return_token_ids, return_tokens_as_token_ids, user, metadata, store, service_tier. Их влияние на hidden generation work проверяет output/multiplier validator; `service_tier` не меняет доверенный priority class. Request-level priority удаляется как сегодня. Любое другое top-level поле в enabled v1 rejected `token_estimation_unsupported`, пока явно не классифицировано. JSON null в optional input fields нормализуется по upstream baseline defaults; для required input null rejected. Для Responses absent/false background допустим, truncation absent/disabled допустим; они не входят в input. Chat response_format всегда fallback, даже если без tools.

Canonical JSON строится из фиксированного whitelist полей выше через детерминированную сериализацию, без whitespace и с UTF-8 вместо HTML escaping. Вложенные значения input structures сохраняются полностью; nested media/remote references проверяются до сериализации. Для batch каждый объект независим. Эта таблица намеренно ограничивает v1, а не обещает все upstream extensions.

### Fallback

`utf8_bytes`: `I = max(1, UTF8ByteLength(canonicalInputJSON) + fallbackOverheadTokens)` на prompt. Canonical input включает все input-bearing fields, роли, инструкции, историю, tools/schema и template options; исключает credentials, telemetry IDs и sampling-only fields. Непонятный input-bearing extension → unsupported, не silent omission. Для Responses canonical input включает instructions и весь inline input. Множитель bytes=1 фиксирован; overhead положительный и калибруемый.

Это оценка для явно включённого fallback, **не математическая гарантия фактического token count произвольной модели**. Input limit в этом режиме применяется к оценке. Exact model-token enforcement требует проверенного upstream adapter. Не включать fallback автоматически после timeout/429/5xx: это скрытый fail-open и обход utility capacity. Fallback применяется только к явно unsupported model/payload/endpoint (включая проверенный 404/501); 401/403 upstream — конфигурационная ошибка 503, не unsupported.

## 6. Estimator и routing

1. После auth, model access, payload/output validation и pool precheck выполнить bounded preflight; до inference admission, без inference counters/circuit permits.
2. Normal mode использует существующий TokenizationCoordinator и `Proxy.Tokenize`. Общие client/backend utility limits учитывают public tokenize + internal preflight. RPM utility списывается один раз за каждый реально приобретённый preflight operation; batch из нескольких prompts делает несколько operations последовательно. Inference RPM до успешного inference admission не списывается.
3. Новый `LLMGW_ADMISSION_ESTIMATE_TIMEOUT=2s`, диапазон (0,30s]; total deadline на весь preflight, не на один prompt. Ответ не более 8 MiB (`LLMGW_ADMISSION_ESTIMATE_MAX_RESPONSE_BYTES`, диапазон 1 KiB..64 MiB); bounded decoding, body закрывается во всех исходах. Не сохранять tokens/token_strs; наружу нужны только count и max_model_len.
4. Validate count ≥0, max_model_len >0, integer bounds, count/tokens consistency если tokens присутствуют. Count missing/null, malformed/truncated response → 503 `token_estimation_unavailable`. `I+O` для каждого prompt не превышает известный model context; ошибку не маскировать как overload.
5. Не считать `/tokenize` универсальным JSON pass-through: adapter строит версионный request из inference fields с теми же chat-formatting defaults. Профиль совместимости на vLLM v0.28.0, pinned в compose; иные версии требуют qualification fixtures.
6. Pool должен быть однородным по model/tokenizer/template/options. `estimationProfile` фиксирует операторское утверждение, которое проверяется acceptance-прогоном для всех backend. Если профиль не квалифицирован, включать pool нельзя организационно; шлюз не может доказать совпадение скрытых server settings только по model name.
7. Нормальная routing/affinity и один pre-byte retry сохраняются в пределах неизменного pool/profile. Оценка с одного backend применима к другому только в этом профиле. Изменение pool model/profile/revision во время preflight → re-resolve/re-estimate максимум один раз; дальше retryable 503. Ни одного повторного Acquire с новым UUID после ambiguous admission outcome в normal mode.
8. Перед dispatch проверить актуальные auth/access, pool/profile и backend identity. После успешного admission новые size-policy limits не применяются к этому lease задним числом; смена model/profile запрещает dispatch старого payload. Retry не создаёт второй token reservation.

## 7. Atomic accounting и lifecycle

В `AdmissionRequest` добавляются immutable `TokenReservation` (enabled, estimate, output allowance, prompt count, n, reserved total, estimator kind/profile, client/pool limits, policy revisions). Fingerprint включает эти поля. Raw payload и даже его hash не включаются в durable storage. Normal lease и reservation — одна admission transaction/critical section, не две независимо успешные операции.

При включённых budgets:

```
poolOutstanding + R <= poolBudget
clientOutstanding + R <= clientBudget  // если clientBudget > 0
```

Budget exhaustion даёт 429 `token_budget_exhausted` + Retry-After; scope=client/pool в bounded telemetry. `R` выше полного budget также 429, с пояснением, что уменьшение запроса/смена policy необходимо: Retry-After не обещает достаточную будущую ёмкость. Проверка всех лимитов предшествует RPM debit/lease insert. Суммы вычисляются в int64 с overflow guard; PostgreSQL SUM(bigint) приводится только после проверки диапазона.

| Событие | Outstanding | TPM / evidence |
| --- | --- | --- |
| Успешный Acquire | +R ровно один раз | RPM один раз; TPM остаётся soft |
| Duplicate Acquire того же fingerprint | Без изменения | Возвращает тот же live lease / terminal rejection |
| Изменённый fingerprint | Без изменения | Idempotency conflict |
| Generation retry | Без изменения | Тот же lease, отдельный backend attempt |
| Renew | Продлить срок R, не изменить величину | Неверный owner не меняет ни одного поля |
| Complete с valid usage | −R один раз | Actual input+output дебитуют soft TPM один раз; фиксируется difference actual−R |
| Complete без valid usage / cancel / transport error | −R один раз после завершения локального lifecycle | Usage=unknown, не actual=0; soft TPM не выдумывает расход |
| Потеря completion / replica crash | R остаётся до TTL | После TTL исключается из sums независимо от cleanup; usage unknown |
| Late completion после expiry | Outstanding не уменьшается второй раз | При retained receipt actual TPM можно применить один раз как сегодня |
| Duplicate completion | Без изменения | Первый terminal completion wins; поздний альтернативный usage не переписывает результат |

Консервативность здесь: весь R удерживается без промежуточных refunds до terminal/expiry; ошибка estimate не уменьшает резерв автоматически. Actual>R не создаёт отрицательные counters и не отменяет доставленный ответ; это telemetry/calibration failure. Успех после early EOS освобождает весь R, а не R−actual: completed work больше не outstanding.

Missing usage не означает бесплатную гарантию потребления: TPM остаётся soft, cancellation может скрыть расход, и TTL не доказывает остановку GPU. Эти ограничения сохраняются явно. Требование prepaid TPM при unknown usage — другой выбранный подход, не скрытая часть реализации.

PostgreSQL хранит reserved amount на request_leases; immutable metadata и reconciliation outcome — на admission_operations. Counts используют expires_at>database-now под прежними pool→client locks. Нет отдельной таблицы суммарных counters, которую надо чинить после crash. Local хранит суммы по client/pool и снимает ровно сохранённый R через общий removeLease path. Во всех Renew/Complete проверяются LeaseID, ClientID, PoolID, RequestID, ReplicaID по сохранённому owner, но не ExpiresAt из старого handle (renewal его меняет).

Local receipt history уже ограничена capacity/retention. Для enabled reservations evicted receipt не должен позволять replay ещё допустимого operation timestamp: хранить bounded per-process replay cutoff, reject older/equal timestamps после eviction. Сначала искать existing receipt/live lease и валидировать fingerprint, затем проверять cutoff только для нового operation. Cutoff продвигается до max OperationStartedAt вытесненных token receipts, но receipt с OperationStartedAt>now нельзя вытеснять до достижения его времени. Если нет допустимого terminal eviction candidate — отказ coordination_unavailable, без продвижения cutoff в будущее. Таким образом future skew до 5m не блокирует все свежие admission на 5m; capacity exhaustion остаётся честным отказом. При совпадении timestamp с cutoff новый operation получает stale_operation; gateway не повторяет его автоматически новым ID. Регресс wall clock ниже cutoff может временно снизить доступность; safety предпочтительнее resurrect. Legacy-only requests не меняют прежний replay contract, но не могут вытеснять protected token receipts в обход правил. PostgreSQL продолжает 24h receipt retention. Полный parity terminal-history retention не обещается.

## 8. Emergency — требование пользователя

При recoverable outage / recovery gate High и Critical сохраняют admission при существующих class/client concurrency caps. Normal/Background не получают emergency. Permanent consistency fault по-прежнему запрещает новые admissions. Новый механизм не лечит readiness и не обходится поверх revoked/disabled identity из последнего доверенного snapshot.

В enabled pool emergency атомарно проверяет существующие emergency concurrency + `R` по client и pool **в одной локальной critical section**. Pool cap = emergencyMaxOutstandingTokens либо pool maxOutstandingTokens; client cap = min(положительный client budget, pool emergency cap), при client=0 — pool emergency cap. Суммы только emergency-запросов этой реплики, включая пережившие recovery. Release idempotent. Возвращение normal mode не обнуляет emergency counters и не переносит их задним числом в PostgreSQL.

Preflight при outage не зависит от недоступного PostgreSQL: отдельный внутренний emergency tokenizer limiter, client concurrency=1, backend concurrency=1, client RPM=30 с burst=1, total estimate timeout=2s (или меньший настроенный normal timeout). Backend health/auth/secret checks сохраняются. Эти defaults конфигурируются `LLMGW_ADMISSION_EMERGENCY_ESTIMATE_CLIENT_MAX_INFLIGHT`, `...BACKEND_MAX_INFLIGHT`, `...CLIENT_RPM` с диапазонами существующего TokenizationPolicy; fingerprint включает их. Public `/tokenize` остаётся на обычном coordinator и не получает emergency.

Emergency tokenization не меняет inference circuit, не пишет inference usage и не расходует durable utility RPM. Это дополнительная per-replica utility capacity при аварии; её нельзя рекламировать как общий utility cap. Если tokenizer недоступен или estimate нельзя получить по выбранной политике — конкретный запрос получает 503; сохранение emergency не означает обход size validation.

Если normal TokenizationCoordinator.Acquire теряет БД до upstream HTTP dispatch, gateway после recheck recoverable status может ровно один раз перейти normal→emergency для eligible High/Critical. Permanent fault, utility 429, upstream timeout/5xx и token_policy_transition такого перехода не разрешают. Total preflight deadline начинается до первой попытки и не перезапускается. Уже полученные batch estimates можно использовать только при том же snapshot/model/profile; failed-to-acquire prompt получает новый **utility** emergency operation ID, normal utility ID не replay через local coordinator. Неизвестный durable utility lease может оставаться до TTL; это входит в объявленный overlap. Если deadline исчерпан, ответ503, не unbounded retry. Весь inference request после такого перехода использует emergency reservation даже если readiness успела восстановиться; повторный permanent/auth/profile check перед dispatch обязателен. Если outage возник после успешного normal preflight, готовый estimate используется в emergency без повторной токенизации при неизменном профиле.

Если normal Acquire закончился неопределённым результатом, возможен оставшийся distributed reservation до TTL и один emergency lifecycle. Генерация стартует не более одного раза со стороны gateway до его обычного upstream retry; повторный normal allocation для того же запроса не выполняется. Такой overlap снижает доступную distributed capacity, но не повторно применяет completion к неизвестному lease. Ghost RPM/lease при ambiguous commit — документировать и проверять.

При N репликах steady shared budget и emergency budgets могут сосуществовать: ориентир outstanding bookkeeping — `B_shared + sum(B_emergency_replica)`, а при снижении policy старые leases могут быть выше новых caps. Это не GPU bound. Глобальный ceiling в outage/recovery не обещается; slot reserve и новый token budget — разные ограничения. Общий pool token budget также не гарантирует token headroom для High: отдельный High token reserve в #23 не добавляется.

## 9. API, telemetry, privacy

Errors используют текущий OpenAI-shaped envelope. 400: `input_token_limit_exceeded`, `output_token_limit_exceeded`, `token_estimation_unsupported` и invalid payload. 429: `token_budget_exhausted`, либо существующие utility/concurrency/rate причины. 503: unavailable estimation/coordination, stale policy/profile и enable transition. Временные 429/503 содержат Retry-After; cancellation сохраняет существующее поведение до/после response commitment.

Добавить numeric-only reservation fields к gateway RequestEvent и отдельный bounded observer accounting event (coordinator/expiry), чтобы видеть releases без HTTP completion. Метрики: estimated/reserved/actual token histograms, outstanding gauge по pool+mode, unknown-usage/underestimate/rejection counters, preflight latency и mode normal/emergency. Gauge distributed берётся из authoritative snapshot; не складывать одинаковый shared gauge разных replicas. Labels: pool, class, estimator kind, scope, mode, bounded reason; никаких request IDs, fingerprints, prompt hashes и произвольных errors.

Actual metrics существующей usage analytics не заполнять оценками. Дубли receipt replay не дают повторного accounting event. Логи только числовые amounts и bounded outcomes. Не сохранять prompts, generation, token strings/IDs, raw tokenizer response, credentials. Проверить sentinel strings в stdout, observer events, SQL metadata и errors. Input/output для вызова upstream существуют только в памяти request lifecycle; кэш промптов не добавляется.

Admin API и существующие client/pool формы показывают enabled, effective limits, class overrides, fallback и emergency per-replica семантику. Существующие редактирования, не знающие новых fields, не должны молча сбрасывать policy: omitted `requestSizeAdmission`/client token fields при update сохраняют прежние значения; явное false/0 выключает/сбрасывает. Create defaults zero. Persisted policy — часть snapshot/revision, не process-only env.

## 10. Acceptance matrix

| ID | Проверяемый критерий | Доказательство |
| --- | --- | --- |
| AC01 | Disabled/default сохраняет payload, прежние admission/TPM/emergency; нет tokenizer calls | HTTP golden + existing suite |
| AC02 | Pool/class/client precedence, invalid values, persistence/reload, partial legacy updates | Domain + SQLite + PostgreSQL + Admin/form tests |
| AC03 | Chat/completion exact adapter, Unicode/tools/templates; Responses явный fallback/reject; missing/invalid count никогда не =0 | Payload fixtures + real model comparison |
| AC04 | Все три endpoints, streaming, output aliases/defaults, n/batch/overflow и unsupported forms | Table-driven HTTP tests |
| AC05 | Client/pool token sums атомарны с concurrency/RPM; same request/replay не выделяет второй резерв | Shared local/PG contract, parallel two-replica test |
| AC06 | Уменьшение лимита, смена класса/профиля, revoke и enable transition не обходят policy | Concurrent publication/DB mutation tests |
| AC07 | Complete/cancel/error/retry/renew/expiry/cleanup не дают утечки или double release; чужой owner не меняет lease | Shared lifecycle matrix + race tests |
| AC08 | Missing usage, under/overestimate, completion backlog drop, late/duplicate completion имеют указанные semantics | Fault injection, fake clock, bounded backlog |
| AC09 | Ambiguous Acquire/Complete и partial committed batch не double-charge; receipts после eviction не resurrect | Existing commit-loss proxy extended + local replay tests |
| AC10 | High/Critical emergency проходит в пределах local token/concurrency caps; Normal/Background reject; tokenizer работает без DB; recovery не сбрасывает active emergency | Service + two HTTP gateways + outage/recovery |
| AC11 | Tokenizer bounded по time/body/concurrency/rate, без generation circuit/analytics side effects | Slow/malformed/oversize response and saturation tests |
| AC12 | Distinct reason/scope/mode, unknown != zero, корректный shared gauge и отсутствие содержимого в persistence/logs | Observer/Prometheus/privacy fixtures |
| AC13 | Migrations zero defaults, contract-version incompatibility, drain upgrade/downgrade и rollback без потери active reservations | Migration + startup tests |
| AC14 | Повторяемое сравнение High admission и TTFT под short-High/long-Background на настоящем vLLM | Real-vLLM report: model/version/hardware/config/seed, enabled vs disabled |

Пример deterministic accounting: Bpool=1000, Bclient=800, R=600 → первый admit, второй этого клиента reject; другой клиент R=400 admit до ровно 1000; R=1 reject; completion первого освобождает 600, replay completion освобождает 0. High не обходит total token/client/RPM/TPM limits.

## 11. Real-vLLM qualification и rollout

Для AC14: фиксированные model/tokenizer/template revisions, vLLM image digest, hardware, scheduling/cache config, gateway commit, topology и все budgets. Две серии disabled/enabled с одинаковыми seeded prompts и load schedule, 3 повтора каждого режима после warm-up, минимум 100 High probe attempts на повтор. Записывать attempted/admitted/rejected по причинам, TTFT p50/p95/p99 **для завершившихся High**, end-to-end latency включая preflight, preflight latency, Background throughput, estimation error, missing usage и peak outstanding. Не сравнивать TTFT только оставшихся successful запросов без admission fraction.

Сценарий обязан реально упереться в token budget (не раньше в utility RPM или concurrency); подобрать нагрузку и зафиксировать calibration до сравнения. Hard pass: expected token denials, eligible High допускается при достаточных всех budgets, output caps соблюдаются, accounting восстанавливается; численное улучшение TTFT не гарантировано. Отчёт обязан показать изменение admission/TTFT и объяснить ухудшение, если оно есть. Synthetic backend не заменяет GPU evidence. Без real-vLLM прогона AC14 остаётся pending, issue не объявлять полностью закрытым.

Миграции: SQLite `006_request_size_admission.sql`; PostgreSQL `000007_request_size_admission.up/down.sql` (перепроверить номера перед реализацией). Persist policy JSON/text с валидацией и сохранением сравнимости domain structs; не добавлять map/slice в ModelPool, который сравнивается через `!=`. Для class overrides использовать fixed struct с четырьмя полями. PostgreSQL coordination contract 3→4. Старые процессы drain/stop перед запуском новой группы; существующий startup wait/unregister применяется. Down migration не удаляет nonzero active token leases/незавершённый контракт. SQLite runtime counters не durable; restart требует прекращения старого процесса, не HA deployment.

## 12. Источники и ограничения анализа

Локальные исходники и тесты на указанной базе; Tier 2 graph navigation с exact source verification. Coverage проверена; no_recorded_issue означает только отсутствие зарегистрированных gaps. Граф может смешивать одноимённые Go methods (например Complete разных coordinators); важные выводы подтверждены чтением файлов.

Upstream baseline: [tokenize protocol v0.28.0](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/serve/tokenize/protocol.py), [chat protocol](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/openai/chat_completion/protocol.py), [completion protocol](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/openai/completion/protocol.py), [Responses protocol](https://github.com/vllm-project/vllm/blob/v0.28.0/vllm/entrypoints/openai/responses/protocol.py). Они подтверждают разные request schemas/output aliases; работоспособность на пользовательском deployment пока не проверена. Новые renderer APIs из latest docs не считаются доступными в baseline и не вводятся скрытой зависимостью.

Связанный план: [implementation plan](../plans/2026-11-10-issue-23-token-reservations.md). Анализ и baseline evidence: [evidence](../../issue-23-analysis-and-evidence.md).
