# AGENTS.md

Mozilla telemetry ingestion system. The repository has thorough developer docs under `docs/` (published to https://mozilla.github.io/gcp-ingestion).

## Repository layout

Monorepo: parent Maven `pom.xml` at the root, Java 11 plus one Python module (`ingestion-edge`).
Components, data flow, and the `ingestion-core` build-helper setup (its sources are co-compiled into `ingestion-beam`/`ingestion-sink`, so editing it re-triggers CI for both) are in `docs/index.md` and `docs/architecture/overview.md`.

## Build & test - Java (`ingestion-beam`, `ingestion-core`, `ingestion-sink`)

Build, test, format, run-from-module, the integration-test scope, and the test-data prerequisites (`bin/download-*` scripts) are in `docs/ingestion-beam/index.md` (prod-data integration workflow: `ingestion_testing_workflow.md`).
Prefer running maven directly (host `mvn` on Java 11) over the `bin/mvn` Docker wrapper - it matches CI and avoids Docker overhead.

Agent-specific gotchas not in those docs:

- Always run `mvn spotless:apply` before claiming Java work is done (formatting violations fail CI).
- A stale `target/` can pass the build (SUCCESS) yet fail at runtime, typically `TypeNotPresentException: AutoValue_*` from stale generated sources. After an IDE build you may also see `java.lang.Error: Unresolved compilation problem` (an IDE artifact; `mvn`/javac never emits it). Fix: `rm -rf ingestion-beam/target ingestion-core/target && mvn test-compile`.
