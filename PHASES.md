# IntegrateTech — Phase Plan

IntegrateTech is a TypeScript + SQLite rebuild of **Forklift** (dcshock/forklift, Java 11, v3.8), a lightweight
Enterprise Service Bus. Developers write small annotated consumer classes and drop them into a deploy
directory. The server then runs them against a message broker, drives every message through a fixed
lifecycle, and lets plugins (retry, replay, stats, metrics) hook into each step.

Status: **Phases 0–4 done.** Phases 0–3 on 2026-11-10; Phase 4 on 2026-11-14. Phases 5–9 not started (the Phase 5 build was removed on 2026-11-20).

## Progress

| Phase | Status | Completed |
|---|---|---|
| 0 — Scaffold and core contracts | ✅ Done | 2026-11-10 |
| 1 — Brokers (SQLite and in-memory) | ✅ Done | 2026-11-10 |
| 2 — Consumer runtime | ✅ Done | 2026-11-10 |
| 3 — Lifecycle plugins, roles, request/response | ✅ Done | 2026-11-10 |
| 4 — Deployment and configuration | ✅ Done | 2026-11-14 |
| 5 — Plugin modules | Not started | — |
| 6 — Server | Not started | — |
| 7 — Admin API and web console | Not started | — |
| 8 — Optional external broker connectors | Not started | — |
| 9 — Examples, docs, hardening | Not started | — |

**Quality gate (all green as of 2026-11-14):**

| Command | Result |
|---|---|
| `npm run typecheck` | Clean |
| `npm run lint` | Clean |
| `npm test` | 202 tests passed in 22 files; 6 repeat runs also clean |

Run the tests from PowerShell or cmd on Windows. Under Git Bash, vitest 5.1 fails every file with "failed to find the current suite", even a one-line test; the project code isn't the cause.

**Test breakdown:**

| Area | Tests |
|---|---|
| Broker conformance suite, run against in-memory, SQLite file and SQLite `:memory:` | 57 (19 × 3) |
| SQLite only: migrations, restart persistence, crash-before-ack recovery, two connectors on one file, durable groups, row cleanup, start failure | 8 |
| In-memory only: copy isolation, injected clock | 2 |
| Core: lifecycle branches, injection, concurrency and ordering, back-off, plugins, plugin registration, request/response, roles, parsers, producer, decorators | 71 |
| Deploy: file scanning and watch (10), properties (5), class deployments (7), worker deployments (8), deployment manager integration (8) | 38 |
| Plugins: stats (3), retry (6), fail → retry → succeed end-to-end (1), replay (8), metrics (8) | 26 |

**Code size (TypeScript, excluding `node_modules`):** about 185 KB of application code (~5,450 lines) and 136 KB of tests (~3,580 lines). There are no comments in the code.

---

## 1. What the source system does

| Area | Forklift (Java) | Source location |
|---|---|---|
| Consumer model | Annotated classes: `@Queue`, `@Topic`, `@GroupedTopic`, `@RoleInput` (plus plural forms), `@OnMessage`, `@OnValidate`, `@On(step)`, `@Response` | `core/.../decorators`, `core/.../source/decorators` |
| Injection | `@Message`, `@Headers`, `@Properties`, `@Config`, `@Producer`, `@Service`/`@Inject` through BeanResolver, constructor injection, named beans | `core/.../consumer/Consumer.java` |
| Lifecycle | Pending → Validating → (Invalid \| Processing → (Error \| Complete)); ack only after processing (at-least-once) | `MessageRunnable.java`, `ProcessStep.java` |
| Concurrency | `@MultiThreaded(n)`, `@Order` for exclusive in-order consumers, bounded pool with caller-runs back-pressure | `ConsumerThread`, `concurrent/*` |
| Plugins | `LifeCycleMonitors.register(classOrInstance)`; `@LifeCycle(step, annotation)` methods | `LifeCycleMonitors.java`, `doc/PLUGINS.md` |
| Producers | `send` overloads (string / object / map / ForkliftMessage, with headers and properties); sync producer that uses `@ResponseUri` and correlation futures | `producers/*` |
| Roles | `@RoleInput` adds a dedicated `forklift-role-<Role>` source so retry and replay can target one consumer | `RoleInputSource`, `consumer/wrapper/*` |
| Deployment | Watches two directories: consumer jars, each in its own child-first classloader, and `.properties` files; hot deploy and undeploy; properties load before consumers | `deployment/*`, `file/*`, `classloader/*` |
| Connectors | ActiveMQ (JMS) and Kafka (consumer groups, Avro, schema registry, rebalance handling) | `connectors/*` |
| Plugin modules | Stats (timings written into properties), Retry (Elasticsearch store, scheduled re-send, `runRetries` master), Replay (async log of every step to Elasticsearch), Datadog (per-step counters and processing timer, toggled per queue) | `plugins/*` |
| Server | CLI flags, config file, state machine LATENT/STARTING/RUNNING/STOPPED/ERROR, embedded mode, Docker | `server/*`, `doc/args-and-config.adoc` |

## 2. Mapping to TypeScript + SQLite

| Forklift piece | IntegrateTech replacement |
|---|---|
| ActiveMQ / Kafka broker | **SQLite-backed broker** (default connector), with queue, topic and grouped-topic semantics, leases, redelivery and a dead-letter queue. In-memory connector for tests |
| Elasticsearch (retry, replay) | SQLite tables `retries` and `replay_log` |
| Kibana (viewing replay logs) | Admin API and web console (Phase 7) |
| Java annotations | TypeScript legacy decorators (`experimentalDecorators`); standard decorators cannot decorate constructor parameters, which constructor injection needs |
| Jar + child-first classloader | Each deployment is a JS/TS bundle loaded with `import()` and a cache-busting URL. `isolation: "worker"` option to run it in a worker thread |
| `.properties` + `@Config` | Same file format, parsed and hot-reloaded |
| Guice/Spring BeanResolver | `BeanResolver` interface (resolve by type token or by name) |
| Datadog plugin | Metrics plugin with pluggable sinks: console, StatsD/DogStatsD UDP, optional Datadog HTTP |
| logback | pino |
| Consul discovery, embedded ActiveMQ | Dropped. Not needed with SQLite |

**Stack:** Node 22+, TypeScript 5.9 (strict), npm workspaces, better-sqlite3 (pinned 12.11.1), vitest 5, eslint 9.

**Layout** (`server/` and `apps/console/` are planned; the rest exist):
```
IntegrateTech/
  packages/
    core/               message model, decorators, consumer runtime, lifecycle, producers, shared broker connector
    conformance/        shared test suite every connector must pass
    connector-sqlite/   SQLite broker
    connector-memory/   in-memory broker for tests
    deploy/             file scanning, properties manager, worker host, deployment manager
    plugin-stats/
    plugin-retry/
    plugin-replay/
    plugin-metrics/
    server/             CLI, config, state machine, admin API
  apps/console/         admin web UI (Phase 7)
  examples/             ported sample consumers
```

---

## 3. Phases

### Summary matrix

| Phase | Scope | Complexity | Status | Main difficulty |
|---|---|---|---|---|
| 0 | Project setup and core types | Low | ✅ Done | Setup and type definitions only; no behavior |
| 1 | SQLite broker and in-memory broker | High | ✅ Done | Leases that hold up under concurrent consumers, three delivery modes, redelivery after a crash, ordered delivery |
| 2 | Consumer runtime | High | ✅ Done | About 15 decorators that store metadata, injection, the full lifecycle, bounded concurrency and graceful stop |
| 3 | Plugin system, roles, request/response | Medium | ✅ Done | Keeping a failing plugin from breaking the rest; matching responses to requests, with timeouts |
| 4 | Hot deploy and properties | High | ✅ Done | Node can't unload ES modules (redeploys leak memory unless isolated in workers), file-watch races, deploy ordering |
| 5 | Stats, retry, replay, metrics plugins | Medium–High | Not started | Retry scheduling that survives restarts, the batching replay writer, four plugins to build |
| 6 | Server | Medium | Not started | Precedence between flags, env vars and config file; the start/stop state machine; draining on shutdown |
| 7 | Admin API and web console | Medium | Not started | Mostly standard API and UI work; the hard part is the per-message lifecycle timeline |
| 8 | Optional ActiveMQ and Kafka connectors | High | Not started | Behavior of real brokers, Kafka rebalancing, test brokers in Docker |
| 9 | Examples, docs, hardening | Low–Medium | Not started | Porting the samples, end-to-end suite, throughput smoke test |

Complexity is relative to the other phases: Low = straightforward, Medium = some tricky parts, High = the riskiest work, where most bugs are expected. Phases 1, 2 and 4 are the critical path.

### Phase 0 — Scaffold and core contracts ✅ Done
- Monorepo, tsconfig (strict, `experimentalDecorators`), eslint, vitest, root scripts: `typecheck`, `lint`, `test`, plus `test:core`, `test:memory`, `test:sqlite`.
- Core types:
  - `IntegrateMessage`: id, body, headers, properties, flagged, warning, `acknowledge()`, `beforeProcessing()`
  - `Header` enum (all 15 Forklift headers, each with a value type)
  - `ProcessStep` enum
  - Source types: `QueueSource`, `TopicSource`, `GroupedTopicSource`, `RoleInputSource`
  - `Connector`, `MessageProducer` and `MessageConsumer` interfaces
  - `ConnectorError`, `ProducerError` and `StartupError`
- **Done when:** `npm run typecheck`, `npm run lint` and `npm test` all pass. ✅ Met.

**Delivered differently from the plan:**
- The producer interface is `MessageProducer`, not `Producer`, because `@Producer` is the injection decorator.
- The planned `ConsumerSource` interface became `MessageConsumer`.

### Phase 1 — Brokers (SQLite and in-memory connectors) ✅ Done
- SQLite schema: `messages`, `subscriptions`, `deliveries` (lease expiry, attempt count), `dead_letters`. WAL mode, versioned migrations.
- Semantics:
  - **Queue:** competing consumers, each message delivered to one consumer.
  - **Topic:** every subscriber gets its own copy.
  - **Grouped topic:** one consumer per group (Kafka-style).
  - **Ordering:** `@Order` is enforced by the consumer runtime (messages with the same order key run one at a time, in arrival order), as in Forklift; the broker itself is plain FIFO within a priority.
- Delivery is at-least-once: a message is leased, then acked or nacked. Expired leases are redelivered. After N attempts the message goes to the dead-letter queue. Delayed delivery is needed for retries.
- Every `send` overload, plus a key/value map body.
- In-memory connector with the same contract. One shared conformance test suite runs against both connectors.
- **Done when:** conformance suite is green for both connectors, including a crash-before-ack redelivery test. ✅ Met: the 19-test conformance suite passes against in-memory, SQLite file and SQLite `:memory:`, and SQLite has 8 tests of its own.

**Delivered differently from the plan:**
- No `destinations` table: queues and topics are named by their deliveries and subscriptions.
- A shared `BrokerConnector` in core holds all broker logic. Each connector only supplies a `BrokerStore`, so the two brokers can't drift apart.
- There is no explicit nack. A consumer releases its leases when it closes, and expired leases are redelivered.
- Extras beyond the plan: `Priority` ordering, `Expiration` drops, abortable receive, and inspection helpers (`depth()`, `deadLetters()`, `openConsumers()`).

**Known limitation:** leases default to 30 s and aren't extended. A handler that runs longer than `leaseMs` will see its message delivered again.

### Phase 2 — Consumer runtime ✅ Done
- Decorators:
  - Sources: `@Queue`, `@Topic`, `@GroupedTopic`, `@RoleInput`, with repeatable forms
  - Handlers: `@OnMessage`, `@OnValidate` (returning `string[]` or `boolean`), `@On(step)` (repeatable)
  - Field injection: `@Message`, `@Headers(h?)`, `@Properties(name?)`, `@Config(file, field?)`, `@Producer(...)`
  - Options: `@MultiThreaded(n)`, `@Order`
- Body marshalling: string, JSON into a class or object, key/value parser, map.
- `MessageRunnable` runs the full lifecycle and records errors. It acks only after processing and closes injected producers.
- `ConsumerThread`: poll loop with back-off, bounded concurrency, graceful stop.
- Services: `BeanResolver` (by type and by name), constructor injection, `@Service`.
- **Done when:** unit tests cover each decorator and lifecycle branch, including validator errors, throwing handlers and a wrong validator return type. ✅ Met.

**Delivered differently from the plan:**
- Repeatable sources work by stacking decorators (`@Queue('a') @Topic('b')`), not by plural forms like `@Queues`.
- Without runtime types, `@Message(as)` takes a mode: `'string'` (the default), `'json'`, `'map'`, `'message'`, or a class.
- `@Properties()` injects all properties; `@Property(name?)` injects one, using the field name when no name is given.
- `@Config` reads an in-memory `PropertiesRegistry`, which also has a full `.properties` parser. Loading and watching files is Phase 4.
- Bad JSON in `@Message` injection logs a warning instead of throwing, so the message isn't redelivered forever (as in Forklift 0.7).
- `IntegrateTech.createConsumers()` and `startConsumers()` start one consumer per source on a class. Phase 4 deployment will build on these.

### Phase 3 — Lifecycle plugins, roles, request/response ✅ Done
- `LifeCycleMonitors.register(classOrInstance)` and `@LifeCycle(step, { annotation })`. A plugin that throws is isolated (the BadAuditor case in the source tests).
- RoleInput wrapper:
  - Adds a `integrate-role-<Role>` source.
  - Unwraps the `RoleInputMessage` envelope.
- `@Response` with `@ResponseUri` (`queue://` and `topic://`).
- `SyncProducer` with a correlation-id resolver and timeouts.
- **Done when:** a request/response round trip and a role-targeted re-send both work in tests. ✅ Met: string, map and object responses (ports of Forklift's `ResponseTest`), topic responses, timeouts, and a plugin re-sending a failed message through the role queue with its id and properties kept.

**Delivered differently from the plan:**
- Plugin annotations are made with `createAnnotation<T>(name)` and passed as `@LifeCycle(step, Annotation)`.
- `SyncProducer.send()` returns `{ correlationId, result }`, and `request()` returns the result directly.
- A malformed role-input envelope is acknowledged and dropped with an error log. Forklift's equivalent threw, which stalled the consumer.

### Phase 4 — Deployment and configuration ✅ Done
Already in place from Phases 2–3: `PropertiesRegistry` and the `.properties` parser, `ConsumerService` with `onDeploy`/`onUndeploy`, the `@Service`/`@CoreService`/`@OnDeploy`/`@OnUndeploy` decorators, and `startConsumers()`.

**Decisions (agreed 2026-11-13):**

| Question | Decision |
|---|---|
| Isolation | Each file bundle runs in its own worker thread. Terminating the worker frees its code. `ClassDeployment` (embedded mode) runs in-process |
| Bundle format | Single `.js` file |
| Change detection | Timer polling, 10 s by default, configurable. No `fs.watch` |
| Redeploy | Finish first: drain the old version's in-flight messages, stop it, then start the new one |
| Properties change | Restart every deployment that reads the changed file |

**Consequence of worker mode:** a worker can't share an in-memory broker with the main thread, so each worker opens its own connector from a serializable spec (for example, a SQLite file path). File deployments therefore need the SQLite connector; the memory connector is for in-process `ClassDeployment` only.

**Tasks:**

| Task | Scope | Complexity |
|---|---|---|
| 4.1 File scanning and watch | `FileScanner` reports added, modified and removed files by size and mtime. A file counts as stable only once two scans in a row see it unchanged, so half-copied files are skipped. `DeploymentWatch` polls both monitor directories on a configurable timer, never runs two scans at once, and stops cleanly | Low–Medium |
| 4.2 Properties manager | `PropertiesManager` loads the `.properties` directory into `PropertiesRegistry`, hot-reloads changed files and removes deleted ones. It records which deployments read which file (from `@Config` metadata) and reports the deployments affected by each change. Properties are loaded before any consumer starts | Medium |
| 4.3 Worker host | Worker entry script: opens a connector from its spec, imports the `.js` bundle, finds the exported consumer classes, applies the properties snapshot and starts consumers. Typed message protocol between main thread and worker: ready, start, drain, stopped, error, log. Bad bundles (import error, no consumers) report an error without crashing the server | High |
| 4.4 Deployment manager | `DeploymentManager` ties 4.1–4.3 together: deploy, redeploy (drain, then swap) and undeploy. A drain timeout force-terminates the worker. Restarts affected deployments on a properties change. Runs `@OnDeploy`/`@OnUndeploy` and `CoreService` hooks. Handles changes in a fixed order (properties first, then bundles) and one change per deployment at a time. `ClassDeployment` registers in-process classes for embedded mode | High |
| 4.5 Integration tests | Temp-directory tests: deploy, edit, redeploy and delete of a bundle and a properties file. Checks that in-flight messages finish before a swap, that a crashing worker is reported and doesn't take others down, that a broken bundle is skipped, and that terminated workers are gone | Medium |

Order: 4.1 and 4.2 are independent, 4.3 depends on neither, 4.4 needs all three, and 4.5 comes last (each task also ships its own unit tests).

- **Done when:** the 4.5 integration tests pass, plus the usual `typecheck`, `lint` and `test` gate. ✅ Met: 8 integration tests in `deployment-manager.test.ts` and 30 unit tests across the other tasks.

**Delivered (package `@integratetech/deploy`):**

| Task | Delivered |
|---|---|
| 4.1 | `FileScanner`, `extensionFilter`, `DeploymentWatch` (`scanNow()` coalesces with a running scan; `start()` scans, waits `settleMs` (default 1 s), then scans again, so files already present deploy at startup) |
| 4.2 | `PropertiesManager` (`apply`, `snapshot`, `track`/`untrack`, `affected`), `propertiesName`, and core `getConfigFiles(class)` |
| 4.3 | `worker-bootstrap.ts`, `worker-host.ts`, `protocol.ts` (`WorkerInit`, `MainToWorker`, `WorkerToMain`, `ModuleSpec`), `WorkerDeployment` |
| 4.4 | `DeploymentManager` (`start`, `stop`, `scan`, `list`, `get`, `on(listener)`, `registerDeployment`, `unregisterDeployment`), `ClassDeployment`, `DeploymentRunner` |

**Delivered differently from the plan:**
- Workers run the TypeScript sources directly. `worker-bootstrap.ts` uses only erasable syntax, so Node 24 runs it as is. It registers synchronous `module.registerHooks` resolve/load hooks that compile `.ts` files with the project's `typescript` (legacy decorators) and resolve extensionless relative imports, then imports `worker-host.ts`. As a result, `typescript` is a runtime dependency of `deploy`.
- Connectors and plugins reach a worker as `ModuleSpec` values (`{ module, export, options }`). The worker imports the module and calls `new Export(options)`. Example: `{ module: '@integratetech/connector-sqlite', export: 'SqliteConnector', options: { filename } }`.
- Core gained `IntegratePlugin` (optional `start(integrate)` and `stop()`) plus `IntegrateTech.registerPlugin()` and `stopPlugins()`. `shutdown()` stops plugins in reverse order before stopping the connector. This is how the plugin specs above, and the Phase 5 plugins, are started and stopped.
- Protocol messages: `start` and `drain` from main to worker; `ready`, `started`, `stopped`, `error` (phase `load`, `start`, `drain` or `runtime`) and `log` from worker to main.
- Deployments are named by file name (`orders.js`). `.mjs` is accepted as well as `.js`. Properties files are keyed by name without `.properties`, matching `@Config('app')`.
- A deployment that fails stays in `list()` with status `failed` and its error. It's retried on the next change to its bundle, or to a properties file it reads. A worker that crashes is not restarted automatically.
- Each change emits an event to `manager.on(listener)`: `deployed`, `redeployed` (reason `bundle` or `properties`, plus `drained`), `undeployed`, `failed` or `properties`.
- `@CoreService` classes are treated like `@Service` inside their own bundle. Workers share no memory, so they can't be shared across deployments.
- connector-sqlite gained `openDatabase()` and `migrateComponent(db, name, migrations)`. Plugin tables can then share a file with the broker without clashing on `user_version`.

### Phase 5 — Plugin modules
- **Stats:** writes per-step timestamps and durations into message properties.
- **Retry** (`@Retry({ maxRetries, timeout, role })`):
  - On Error, store the message in the SQLite `retries` table and schedule a re-send to the role source after `timeout`.
  - Only an instance started with `runRetries` re-sends at startup.
  - Delete the entry on success. Mark it `max-retries-exceeded` when the limit is hit.
- **Replay** (`@Replay({ role })`):
  - Log every lifecycle step for each message to `replay_log`, through a batching async writer that flushes on a timer.
  - Idempotent replay consumer.
  - Re-send API.
- **Metrics:** counters per step and a processing timer, tagged by environment, host and queue. Per-queue toggles from config, e.g. `metrics.lifecycle.counter.pending.<queue>`.
- **Done when:** each plugin's tests pass, and an end-to-end flow (fail → retry → succeed) leaves the expected replay rows.

### Phase 6 — Server
- CLI: `integratetech-server`. Flags follow Forklift's `-monitor1`, `-monitor2`, `-url` (SQLite path, or `embedded`), `-username`/`-password`, `-configurationFile`, `-runRetries`, `-retryDb`/`-replayDb`, and the metrics sink and keys flags.
- Config file plus env vars. Flags win over env vars, and env vars win over the file.
- `ServerState` state machine. `startServer(wait)` and `stopServer(wait)`. SIGINT/SIGTERM drain.
- Embedded API: `new IntegrateTech().start(connector)` and `registerDeployment(...classes)`.
- Structured logging (pino). Dockerfile.
- **Done when:** the CLI boots, deploys an example bundle, processes messages, and shuts down cleanly. `--help` lists every flag.

### Phase 7 — Admin API and web console *(new, replaces Kibana)*
- REST endpoints:
  - health and server state
  - deployments and consumers (with live counts)
  - destination depth and dead-letter queue
  - replay log search
  - retry queue
  - re-send or replay a message
  - publish a test message
- Token or password auth, matching the source's broker-password support.
- Small React (Vite) console: overview, destinations, a message's lifecycle timeline, retries, dead-letter queue.
- **Done when:** API tests pass and the console can view and replay a failed message.

### Phase 8 — Optional external broker connectors
- ActiveMQ over STOMP, and Kafka (kafkajs) with consumer groups, ack batching and rebalance handling, behind the same `Connector` contract. Each must pass the Phase 1 conformance suite against a real broker in Docker.
- Avro and the schema registry stay out of scope unless needed.

### Phase 9 — Examples, docs, hardening
- Port the sample consumers: TestQueueConsumer, MultiQ/MultT/QueueTopic, and JarJar as an isolation demo.
- Quickstart, decorator reference, plugin authoring guide.
- End-to-end suite and a throughput smoke test.
- **Done when:** the quickstart works from a clean clone.

---

## 4. Defaults chosen (open to change)
1. The SQLite broker is the default and only required broker. Real ActiveMQ and Kafka come in optional Phase 8.
2. TypeScript legacy decorators (`experimentalDecorators`). Standard decorators were the first choice but cannot decorate constructor parameters.
3. Everything is stored in SQLite. Elasticsearch, Consul and the embedded ActiveMQ broker are dropped.
4. The admin console (Phase 7) is new, not a port. It stands in for Kibana.
5. Not carried over from the dump: `project/.gnupg/*.gpg` keys, Travis encrypted secrets, the Datadog keys in the docs example, sbt/gradle build files.
