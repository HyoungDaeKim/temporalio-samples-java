# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository purpose

Samples demonstrating the [Temporal Java SDK](https://github.com/temporalio/sdk-java). Each sample is self-contained and independently runnable — this is not a library, so favor clarity over cross-sample abstraction.

## Prerequisites

- Java 17+
- A running Temporal server on `127.0.0.1:7233` (default). Start locally with `temporal server start-dev` (Temporal CLI).

## Common commands

Build and run all tests:

```
./gradlew build
```

Run a single Gradle module's tests (e.g. `core`):

```
./gradlew :core:test
```

Run a single test class or method:

```
./gradlew :core:test --tests io.temporal.samples.hello.HelloActivityTest
./gradlew :core:test --tests io.temporal.samples.hello.HelloActivityTest.testActivity
```

Run any `core` sample by its `main` class — this is the convention used in every sample's README:

```
./gradlew -q execute -PmainClass=io.temporal.samples.hello.HelloActivity
./gradlew -q execute -PmainClass=<fqcn> -Pargs="arg1 arg2"
```

Run Spring Boot samples (browse UI at http://localhost:3030):

```
./gradlew :springboot:bootRun
./gradlew :springboot-basic:bootRun
./gradlew :springboot:bootRun --args='--spring.profiles.active=tc'   # Temporal Cloud profile
```

Run Spring AI samples (each is a separate Boot app with an interactive CLI; needs `OPENAI_API_KEY`, and `ANTHROPIC_API_KEY` for `multimodel`):

```
./gradlew :springai:basic:bootRun
./gradlew :springai:mcp:bootRun
./gradlew :springai:multimodel:bootRun
./gradlew :springai:rag:bootRun
```

Formatting and lint:

- `spotlessApply` runs automatically before `compileJava` (google-java-format). CI runs `./gradlew spotlessCheck` — do not bypass it.
- `-Werror` is enabled for all subprojects, and Error Prone is applied. Warnings fail the build.

## Module layout and version-skew gotcha

The build is a multi-module Gradle project. `settings.gradle` includes: `core`, `springboot`, `springboot-basic`, and four `springai:*` submodules.

- **`core/`** — the bulk of samples. Plain `main` methods, run via `execute -PmainClass=...`. Adds gRPC codegen (protobuf plugin) and pulls in the full grab-bag of demo dependencies (AWS Encryption SDK, CloudEvents, Jackson JQ, OpenTelemetry/Jaeger, etc.). Root package: `io.temporal.samples.<sample-name>`.
- **`springboot/`** and **`springboot-basic/`** — Spring Boot autoconfig via `temporal-spring-boot-starter`. Default to **Spring Boot 2.7.13**; to switch to Boot 3.x, uncomment the alternate line in `gradle.properties`.
- **`springai/*`** — durable Spring AI agents via `temporal-spring-ai`. Shared config lives in `gradle/springai.gradle`.

**Important version-skew note (documented in `gradle/springai.gradle`):** the root `build.gradle` declares the `org.springframework.boot` Gradle plugin at `${springBootPluginVersion}` (2.7.13), but Spring AI 1.1.0 requires Spring Boot 3.5.x, which the `springai:*` modules import via BOM (`spring-boot-dependencies:3.5.3` + `spring-ai-bom:1.1.0`). The plugin version (task wiring) and the BOM version (dependency versions) are intentionally independent — do not "fix" this mismatch without also migrating the legacy `springboot/` samples off Boot 2.7.

Cross-module dep versions are set via `subprojects { ext { … } }` in the root `build.gradle` (`javaSDKVersion`, `otelVersion`, `camelVersion`). Bump SDK versions there, not per-module.

## Sample conventions

Nearly every `core` sample follows the same skeleton in a single file:

1. Constants for `TASK_QUEUE` and `WORKFLOW_ID`.
2. `@WorkflowInterface` + `@WorkflowMethod` on the workflow interface; static inner `…Impl` class.
3. `@ActivityInterface` + optional `@ActivityMethod`; static inner `…Impl` class.
4. `main` — loads config via `ClientConfigProfile.load()`, builds `WorkflowServiceStubs` → `WorkflowClient` → `WorkerFactory`, registers workflow/activity impls on a worker, starts the factory, then creates a workflow stub and invokes it.

New samples should preserve this shape (single-file, no shared framework code) unless there is a specific reason not to. Samples must not depend on each other.

Client connection defaults come from `io.temporal.envconfig.ClientConfigProfile` — samples pick up env/config file overrides automatically; do not hardcode host/namespace.

## Testing conventions

Tests live in `core/src/test/java/...` mirroring the sample package. Both JUnit 4 (`junit:junit:4.13.2`) and JUnit 5 (`junit-jupiter`) are on the classpath — many samples have both a `FooTest` (JUnit 4) and a `FooJUnit5Test` variant to demonstrate both styles. Gradle uses the JUnit Platform (`useJUnitPlatform()`), which runs both via the vintage engine.

Tests use `io.temporal:temporal-testing` (`TestWorkflowEnvironment`) — they do **not** require a running Temporal server. `./gradlew build` in CI runs them via `docker/github/docker-compose.yaml`.

## CI

Two jobs in `.github/workflows/ci.yml`:
- `unittest` — runs `./gradlew --no-daemon test` inside the docker-compose stack.
- `code_format` — runs `./gradlew --no-daemon spotlessCheck`. Run `./gradlew spotlessApply` locally before pushing if you skipped a build.
