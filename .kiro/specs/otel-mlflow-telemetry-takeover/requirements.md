<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Requirements Document — OTel + MLflow Telemetry Takeover

## Introduction

This spec takes over [ai-dynamo/aiperf#656](https://github.com/ai-dynamo/aiperf/pull/656)
("feat: Live telemetry and artifact upload to MLflow"), authored by Emmanuel
Bashorun (`briefgaming`). The PR adds three user-facing capabilities to AIPerf:

1. **Live OpenTelemetry (OTel) metrics streaming** during a benchmark run via a
   new `OTelMetricsResultsProcessor` and a background fanout process.
2. **Live MLflow logging** of metrics during a run, plus **post-run MLflow
   artifact upload** of the JSON/CSV outputs already produced by the local
   exporters.
3. **`aiperf plot --mlflow-upload`** to attach rendered plots to an MLflow run
   (live or historical).

The PR has been through two rounds of CodeRabbit review plus inline maintainer
review (`ajcasagrande`). It is currently **mergeable but unstable** against
`main` (base `8763c57c`, head `628162da`, `main` is `5b04befe`, five commits
ahead). Round-2 review confirmed five functional defects and a handful of
round-3 style issues; the rebase against `main` is expected to produce
conflicts in `user_config.py`, `records_manager.py`, `exporter_manager.py`, and
`plugins.yaml`.

The goal of this takeover is to land a **single, clean, reviewable PR** that:

- Preserves the original scope (nothing new is added; nothing already approved
  is removed).
- Preserves the original author's attribution via
  `Co-authored-by: Emmanuel Bashorun <bashorun.emma@gmail.com>`.
- Fixes every confirmed defect from rounds 2 and 3 and any unresolved inline
  review items.
- Conforms to current repo conventions (Python 3.10+, `orjson`, `X | Y` union
  syntax, `AIPerfBaseModel`/`BaseConfig`, enums accessed without `.value`,
  `uv`/`make`/`pre-commit` workflow).
- Ships complete documentation (architecture, patterns, metrics reference,
  tutorial, CLI/env auto-docs, plugin artifacts, Four-File Sync).
- Ships property-based and integration tests that make the defect fixes
  regression-proof.

This document is the contract for "done." A reader new to the repo should be
able to take Requirement N, check the referenced code and review URLs, and
know exactly what "done" looks like.

## Glossary

- **AIPerf**: The Python 3.10+ async benchmarking tool in this repo; nine
  services communicate over a ZMQ message bus. See
  [`docs/architecture.md`](../../../docs/architecture.md).
- **PR 656**: [github.com/ai-dynamo/aiperf/pull/656](https://github.com/ai-dynamo/aiperf/pull/656),
  the PR being taken over. Head SHA `628162da`, base SHA `8763c57c`.
- **Records_Manager**: The `RecordsManager` service
  (`src/aiperf/records/records_manager.py`) that aggregates metrics from
  record processors and drives results processors.
- **Results_Processor**: A plugin that consumes per-record aggregated metrics
  from `Records_Manager`; registered under the `results_processor` plugin
  category in [`src/aiperf/plugin/plugins.yaml`](../../../src/aiperf/plugin/plugins.yaml).
- **OTel_Metrics_Results_Processor**: New class
  `aiperf.post_processors.otel_metrics_results_processor.OTelMetricsResultsProcessor`
  introduced by PR 656. Runs inside `Records_Manager` and pushes records into
  a multiprocessing queue consumed by the Fanout_Process.
- **Fanout_Process**: A child process started by
  `OTelMetricsResultsProcessor` and implemented in
  `aiperf.post_processors.otel_streaming_fanout`. Owns all OTel export clients
  and (when MLflow live logging is enabled) owns the live MLflow sink.
- **OTel_Collector**: Any OpenTelemetry Protocol (OTLP) HTTP metrics endpoint
  reachable via `--otel-url`. Normalization is handled by
  `_normalize_otel_metrics_url`.
- **MLflow_Live_Sink**: The in-process, fanout-owned MLflow logger used
  during a run. Writes metric scalars via `mlflow.log_metric(...)`.
- **MLflow_Data_Exporter**: The post-run data exporter
  `aiperf.exporters.mlflow_data_exporter.MLflowDataExporter`. Runs from
  `ExporterManager` after local file exporters and uploads artifacts
  (`profile_export_*`, `inputs.json`, etc.) to an MLflow run.
- **Deferred_MLflow_Path**: The code path in
  `ExporterManager.export_data` that runs `MLflowDataExporter` after the
  local exporters have finished writing their files.
- **MLflow_Metadata_File**: `mlflow_export.json`, written by the live sink,
  containing at least `{tracking_uri, experiment_name, run_id, run_name,
  benchmark_id, uploaded_artifacts, reused_live_run}`. Used by the post-run
  exporter to decide whether to reuse the live run.
- **Plot_CLI**: `aiperf plot`, implemented in
  `src/aiperf/cli_commands/plot.py` and `src/aiperf/plot/cli_runner.py`.
- **Strategy_Protocol**: The `ResultsStrategy` protocol introduced by PR 656
  under `src/aiperf/post_processors/strategies/`, with concrete strategies
  `MetricResultsStrategy` and `TimingResultsStrategy`.
- **Four_File_Sync_Rule**: `AGENTS.md`, `CLAUDE.md`,
  `.github/copilot-instructions.md`, and `.cursor/rules/python.mdc` must
  contain identical content. Enforced by `make check-agent-files-sync` and
  the `check-agent-files-sync` pre-commit hook.
- **PBT**: Property-based testing (Hypothesis in this repo).
- **Round-2 Review**: The second CodeRabbit review on PR 656 that surfaced
  the five functional defects listed in Requirement 7.
- **Round-3 Style Fixes**: The third CodeRabbit review plus
  `ajcasagrande`'s inline comments covering type annotations, stale
  comments, and copy tweaks.

## Requirements

### Requirement 1: Preserve and Complete Live OTel Metrics Streaming

**User Story:** As an AIPerf operator, I want AIPerf to stream live metric
deltas to my OTel Collector during a run so that I can watch dashboards
update in real time without waiting for the run to finish.

#### Acceptance Criteria

1. WHERE `--otel-url <URL>` is provided on the `aiperf profile` command,
   THE AIPerf SHALL register `OTel_Metrics_Results_Processor` with
   `Records_Manager` as an additional Results_Processor.
2. WHEN `OTel_Metrics_Results_Processor` starts, THE AIPerf SHALL spawn
   exactly one Fanout_Process that owns all OTel SDK exporters and, when
   enabled, the MLflow_Live_Sink.
3. WHEN a value of `--otel-url` is a bare host, `host:port`, or
   `http(s)://host[:port][/path]`, THE `_normalize_otel_metrics_url`
   SHALL produce a URL that ends with the path segment `/v1/metrics`
   exactly once and preserves the original host and port.
4. IF `--otel-url` uses a scheme other than `http` or `https`, THEN THE
   AIPerf SHALL reject the URL with a descriptive configuration error
   before Fanout_Process is spawned.
5. WHERE `--stream metrics` is selected, THE `OTel_Metrics_Results_Processor`
   SHALL emit OTel records built from `MetricResultsStrategy`; WHERE
   `--stream timing` is selected, THE processor SHALL use
   `TimingResultsStrategy`; WHERE `--stream default` or the flag is
   omitted, THE processor SHALL select the legacy strategy currently
   used by the metrics path on `main`.
6. THE `OTel_Metrics_Results_Processor` SHALL drop entries whose coerced
   numeric value list is empty after `coerce_metric_values` rather than
   sending empty payloads to Fanout_Process.
7. THE AIPerf SHALL NOT import any OpenTelemetry SDK symbol at module
   import time outside the optional-dependency guard defined in
   `src/aiperf/common/optional_dependencies.py`; the guard message
   SHALL keep `pip install aiperf[otel]` as the primary hint per
   maintainer `ajcasagrande`
   ([pull/656#discussion_r2561](https://github.com/ai-dynamo/aiperf/pull/656)).
8. `EndpointConfig.model_names` SHALL enforce `min_length=1` because
   `OTel_Metrics_Results_Processor._build_resource_attributes` indexes
   `model_names[0]`; submitting `--model-names` empty SHALL fail
   validation with a clear message.

### Requirement 2: Preserve and Complete Fanout Process Architecture

**User Story:** As an AIPerf operator running at sustained load, I want the
fanout process to absorb bursts without blocking the hot path or exhausting
memory so that my benchmark results are not distorted by telemetry.

#### Acceptance Criteria

1. THE Fanout_Process SHALL use a `multiprocessing.Queue` whose `maxsize`
   equals `AIPERF_OTEL_MAX_BUFFERED_RECORDS` (default `10000`).
2. IF the fanout queue is full when a new record arrives, THEN THE
   `OTel_Metrics_Results_Processor` SHALL drop the oldest buffered
   record, enqueue the new record, and increment
   `_fanout_dropped_events` by exactly one.
3. THE AIPerf SHALL expose `AIPERF_OTEL_FLUSH_INTERVAL_SECONDS`
   (default `2.0`), `AIPERF_OTEL_MAX_BATCH_RECORDS` (default `500`),
   `AIPERF_OTEL_MAX_BUFFERED_RECORDS` (default `10000`), and
   `AIPERF_OTEL_REQUEST_TIMEOUT_SECONDS` (default `10.0`); each SHALL
   be readable from `aiperf.common.environment` and documented in
   `docs/environment-variables.md`.
4. WHEN Fanout_Process receives a shutdown sentinel from
   `OTel_Metrics_Results_Processor`, THE Fanout_Process SHALL drain
   the remaining queue, flush any pending batches and MLflow metrics,
   close SDK exporters, and exit within
   `AIPERF_OTEL_REQUEST_TIMEOUT_SECONDS + 5` seconds.
5. IF Fanout_Process encounters a transient OTLP export error, THEN THE
   process SHALL log the error at warning level and continue; records
   already in the queue SHALL NOT be lost due to a single export
   failure.

### Requirement 3: Preserve and Complete Live MLflow Metrics Logging

**User Story:** As an AIPerf operator, I want per-metric scalars to appear in
my MLflow experiment in near real time during a run so I can compare live
dashboards against MLflow history.

#### Acceptance Criteria

1. WHERE `--mlflow` is provided and `--otel-url` is provided,
   THE Fanout_Process SHALL log metric deltas to the configured MLflow
   tracking URI inside the active MLflow run.
2. WHERE `--mlflow-tracking-uri`, `--mlflow-experiment`,
   `--mlflow-run-name`, or `--mlflow-tag` are provided, THE
   Fanout_Process SHALL apply those values when creating or resuming
   the MLflow run.
3. THE Fanout_Process SHALL write `MLflow_Metadata_File` at
   `{output_dir}/mlflow_export.json` containing at least
   `tracking_uri`, `experiment_name`, `run_id`, `run_name`,
   `benchmark_id`, and initially empty `uploaded_artifacts` and
   `reused_live_run` fields; writes SHALL use `orjson.dumps`.
4. IF the MLflow Python package is not installed while `--mlflow` is
   passed, THEN THE AIPerf SHALL fail fast with the optional-dependency
   hint from `install_optional_dependency_hint("mlflow")` whose text
   SHALL start with `pip install aiperf[mlflow]`.
5. WHERE `--mlflow` is set WITHOUT `--otel-url`, THE AIPerf SHALL still
   defer MLflow artifact upload to post-run (see Requirement 4) and
   SHALL NOT require Fanout_Process to spawn.

### Requirement 4: Post-Run MLflow Artifact Upload (Deferred Exporter)

**User Story:** As an AIPerf operator, I want the JSON and CSV files that
AIPerf already writes locally to be attached to my MLflow run as artifacts
after the run ends so I have a single source of truth in MLflow.

#### Acceptance Criteria

1. WHEN `ExporterManager.export_data` completes the local exporters
   phase, THE `MLflow_Data_Exporter` SHALL run in the
   Deferred_MLflow_Path.
2. WHERE `MLflow_Metadata_File` exists AND its `tracking_uri` matches
   the configured tracking URI AND its `benchmark_id` matches the
   current benchmark ID, THE `MLflow_Data_Exporter` SHALL reuse the
   live run_id and SHALL set `reused_live_run = true` in the final
   local copy of `MLflow_Metadata_File`.
3. WHERE no matching `MLflow_Metadata_File` exists, THE
   `MLflow_Data_Exporter` SHALL create a new MLflow run using the CLI
   flags under `--mlflow-*`.
4. THE set of uploaded artifacts SHALL include each file whose path
   matches any glob returned by the
   `mlflow_resolved_artifact_globs` property whose return type SHALL
   be `list[str] | tuple[str, ...]` and SHALL NOT be the invalid
   `tuple[str]`.
5. WHEN all artifacts have been uploaded, THE `MLflow_Data_Exporter`
   SHALL update the local `MLflow_Metadata_File` with the final
   `uploaded_artifacts` list AND `reused_live_run` flag BEFORE
   uploading the metadata file itself, so the artifact stored in
   MLflow equals the final local copy
   (round-2 defect: stale uploaded `mlflow_export.json`).
6. IF the same MLflow run already contains a previously uploaded
   `mlflow_export.json`, THEN THE `MLflow_Data_Exporter` SHALL
   overwrite it so that the final value observed in MLflow matches
   the final local copy.
7. THE Deferred_MLflow_Path SHALL NOT duplicate functionality provided
   by any separate `export_mlflow()` helper; duplication identified
   in [CodeRabbit review 2867380366](https://github.com/ai-dynamo/aiperf/pull/656#pullrequestreview-2867380366)
   SHALL be collapsed into a single code path before merge.

### Requirement 5: Strategy-Driven Results Processors

**User Story:** As an AIPerf developer, I want one Results_Processor to serve
both metrics and timing telemetry via pluggable strategies so that the fanout
hot path stays small and future streaming modes are cheap to add.

#### Acceptance Criteria

1. THE `Strategy_Protocol` SHALL be defined in
   `src/aiperf/post_processors/strategies/core.py` and SHALL declare
   the minimal interface used by `OTel_Metrics_Results_Processor`
   (e.g., `build_records`, `flush_tail`, `coerce_metric_values`).
2. THE `MetricResultsStrategy` SHALL live in
   `src/aiperf/post_processors/strategies/metric_results.py` and
   SHALL cover the behavior formerly inlined in the legacy metrics
   path.
3. THE `TimingResultsStrategy` SHALL live in
   `src/aiperf/post_processors/strategies/timing_results.py` and
   SHALL emit metrics under the `aiperf.timing.*` namespace.
4. `src/aiperf/post_processors/strategies/__init__.py` SHALL re-export
   `MetricResultsStrategy`, `TimingResultsStrategy`, and the
   `Strategy_Protocol` identifier used by `OTel_Metrics_Results_Processor`.
5. THE `post_processors/protocols.py` update introduced by PR 656
   SHALL remain backwards-compatible with existing
   `record_export_results_processor` and
   `metric_results_processor` implementations; `uv run pytest tests/unit/post_processors -n auto`
   SHALL pass on `main`.

### Requirement 6: `aiperf plot --mlflow-upload`

**User Story:** As an AIPerf operator, I want to attach plots rendered from
an existing export to an MLflow run so that the visual artifacts live
alongside the metric scalars.

#### Acceptance Criteria

1. WHERE `aiperf plot --mlflow-upload` is invoked, THE Plot_CLI SHALL
   require either `--mlflow-run-id` OR a valid `MLflow_Metadata_File`
   present in the input directory.
2. WHERE `--mlflow-tracking-uri` is provided on `aiperf plot`, THE
   Plot_CLI SHALL use that URI instead of any URI read from
   `MLflow_Metadata_File`.
3. THE Plot_CLI SHALL upload every rendered plot file as an MLflow
   artifact and SHALL log a line stating the run ID and artifact
   count.
4. THE Plot_CLI SHALL use `orjson.loads`/`orjson.dumps` for any JSON
   I/O it performs (including reading `MLflow_Metadata_File`), per
   repo convention.

### Requirement 7: Round-2 Confirmed Defect Fixes

**User Story:** As an AIPerf maintainer, I want every functional defect
flagged in CodeRabbit's round-2 review on PR 656 to be fixed and covered
by tests so that the defects cannot silently regress.

#### Acceptance Criteria

1. WHEN records are flowing into Fanout_Process at sustained load,
   THE MLflow_Live_Sink SHALL flush buffered metrics both when the
   buffered count reaches `AIPERF_OTEL_MAX_BATCH_RECORDS` AND when
   `AIPERF_OTEL_FLUSH_INTERVAL_SECONDS` of wall-clock time has
   elapsed since the last flush, regardless of whether the queue is
   currently idle (round-2 defect: `_flush_mlflow_metrics` starvation
   in `otel_streaming_fanout.py`).
2. THE MLflow_Live_Sink SHALL record **cumulative** per-
   `(metric, attribute_key)` snapshots for timing telemetry, even
   though `TimingResultsStrategy` emits deltas to the OTel up/down
   counter (round-2 defect: timing gauges logged as deltas produce
   negative/oscillating MLflow values in `timing_results.py`).
3. THE final local `MLflow_Metadata_File` uploaded to MLflow SHALL
   equal the final local copy on disk bit-for-bit, including the
   `uploaded_artifacts` list and `reused_live_run` flag
   (round-2 defect: stale uploaded `mlflow_export.json`).
4. THE fanout queue `maxsize` SHALL be controlled by
   `AIPERF_OTEL_MAX_BUFFERED_RECORDS`; changing the env var SHALL
   change observed capacity (round-2 defect: hard-coded queue size
   made the env var inert).
5. IF `--dashboard` AND `--mlflow-upload` are combined, THEN THE
   AIPerf SHALL reject the configuration with a `ValueError` BEFORE
   `controller.run()` is invoked; no server/controller start-up
   work SHALL be performed (round-2 defect: validation happened
   after blocking controller startup).

### Requirement 8: Round-3 Style and Type Fixes

**User Story:** As an AIPerf maintainer, I want the nit-level issues flagged
in CodeRabbit round-3 and inline maintainer comments to be resolved so that
the final diff is clean.

#### Acceptance Criteria

1. THE return type annotation of `mlflow_resolved_artifact_globs`
   SHALL be `list[str] | tuple[str, ...]` and SHALL NOT use the
   invalid `tuple[str]`.
2. THE stale comment `# Flushes any buffered data` SHALL be removed
   from `src/aiperf/records/records_manager.py`.
3. THE text returned by `install_optional_dependency_hint` SHALL lead
   with `pip install aiperf[<extra>]` (not `uv add`), because the
   hint targets end-users, per maintainer `ajcasagrande`.
4. No code under `src/aiperf/**` SHALL reference the renamed property
   `otel_streaming_enabled`; all call sites SHALL use
   `otel_collector_enabled`; `grepSearch` for the old name SHALL
   return zero hits.
5. All new or modified JSON I/O introduced by this PR (including
   plot CLI and tests) SHALL use `orjson.loads` / `orjson.dumps`;
   usage of the stdlib `json` module in new code SHALL be limited to
   loading third-party schemas or test fixtures that already live in
   the repo.
6. All new Python modules SHALL use `X | Y` instead of
   `Optional[X]` / `Union[X, Y]`; all new Pydantic fields SHALL carry
   `Field(description=...)`; all new enums SHALL be accessed without
   `.value`.

### Requirement 9: Rebase onto Current `main` Without Losing Author Attribution

**User Story:** As an AIPerf maintainer, I want the takeover PR to merge
cleanly on top of the current `main` tip while preserving the original
author's credit so that the project history reflects both contributors.

#### Acceptance Criteria

1. THE takeover branch SHALL be rebased (or its commits cleanly
   re-applied) onto the current tip of `main` (`5b04befe` at the
   start of this spec; the actual target is the `main` HEAD at the
   moment of PR creation).
2. THE rebase SHALL resolve conflicts in at least
   `src/aiperf/common/config/user_config.py`,
   `src/aiperf/records/records_manager.py`,
   `src/aiperf/exporters/exporter_manager.py`, and
   `src/aiperf/plugin/plugins.yaml` without reverting any change
   landed on `main` after `8763c57c`.
3. THE resulting branch SHALL contain a small number of
   Conventional-Commit messages (ideally one `feat(...)` plus
   follow-up `fix(...)`/`docs(...)`/`test(...)` commits). A single
   squashed `feat(telemetry): live OTel metrics + MLflow export`
   commit IS acceptable.
4. EVERY commit on the takeover branch SHALL include
   `Co-authored-by: Emmanuel Bashorun <bashorun.emma@gmail.com>` and
   SHALL be DCO-signed (`git commit -s`).
5. THE takeover branch SHALL follow the `<username>/feature-name`
   convention. WHERE push access exists on the original
   `briefgaming/aiperf:feat/otel-mlflow-tracking` branch (the PR
   allows `maintainer_can_modify`), THE takeover SHALL update PR
   #656 in place. WHERE push access is unavailable, THE takeover
   SHALL open a new PR from `<current-user>/otel-mlflow-tracking`
   that links to and closes PR #656.
6. THE takeover branch SHALL NEVER be force-pushed to a remote
   branch that already exists on `ai-dynamo/aiperf`; history
   rewriting SHALL stay local per repo git policy.

### Requirement 10: Documentation Updates

**User Story:** As an AIPerf user discovering this feature, I want
documentation on every user-facing surface and on the internal architecture
so that I can learn the feature without reading source.

#### Acceptance Criteria

1. THE `docs/cli-options.md` file SHALL be regenerated by
   `make generate-all-docs` (which runs `make generate-cli-docs`);
   manual edits to this file SHALL NOT be present in the final diff.
2. THE `docs/environment-variables.md` file SHALL be regenerated by
   `make generate-all-docs` and SHALL include every new
   `AIPERF_OTEL_*` variable from Requirement 2.
3. THE plugin artifact files under `src/aiperf/plugin/**` SHALL be
   regenerated by `make generate-all-plugin-files`; `make validate-plugin-schemas`
   SHALL pass.
4. THE `docs/architecture.md` SHALL describe the Fanout_Process, the
   `--stream` dispatch through `Strategy_Protocol`, and the
   Deferred_MLflow_Path post-run export.
5. THE `docs/dev/patterns.md` SHALL document the Strategy_Protocol +
   `MetricResultsStrategy`/`TimingResultsStrategy` pattern and the
   fanout queue with drop-oldest backpressure.
6. THE `docs/metrics-reference.md` SHALL document the new
   `aiperf.timing.*` metric namespace produced by
   `TimingResultsStrategy`.
7. THE repo SHALL include a new tutorial file under
   `docs/tutorials/` (e.g. `docs/tutorials/otel-mlflow.md`) that
   walks an end-user through `--otel-url`, `--mlflow`, and
   `aiperf plot --mlflow-upload` end-to-end.
8. THE new tutorial SHALL be listed in `README.md`'s tutorial index
   AND in `docs/index.yml`; `tools/check_docs_index.py` SHALL pass.
9. `AGENTS.md`, `CLAUDE.md`, `.github/copilot-instructions.md`, and
   `.cursor/rules/python.mdc` SHALL remain byte-identical save for
   headers/frontmatter; `make check-agent-files-sync` SHALL pass.
10. THE `README.md` SHALL describe the new optional extras
    `aiperf[mlflow]`, `aiperf[otel]`, and `aiperf[mlflow,otel]`.

### Requirement 11: Tests (Unit, Property-Based, Integration)

**User Story:** As an AIPerf maintainer, I want every new behavior and every
round-2 defect covered by tests so that the feature is regression-proof.

#### Acceptance Criteria

1. `uv run pytest tests/unit -n auto` SHALL pass on the takeover
   branch without new xfail markers.
2. THE unit test suite SHALL include a property-based test for
   `_normalize_otel_metrics_url` asserting that for any input of
   the form `host`, `host:port`, `http://host[:port][/path]`, or
   `https://host[:port][/path]`, the normalized result ends with
   the path segment `/v1/metrics` exactly once and preserves the
   original host and port; non-`http(s)` schemes SHALL raise.
3. THE unit test suite SHALL include a property-based test for
   `coerce_metric_values` asserting that booleans are dropped,
   numeric scalars produce a singleton list, mixed lists retain
   only numeric-non-boolean entries, and unsupported types produce
   an empty list.
4. THE unit test suite SHALL include a property-based test for the
   timing counter delta accounting: given a sequence of cumulative
   counter snapshots with tolerated resets, the emitted deltas are
   non-negative (or equal to the new value on reset) and sum to
   the final cumulative value.
5. THE unit test suite SHALL include a property-based test for the
   new MLflow timing gauge path: the cumulative snapshot per
   `(metric, attribute_key)` observed by MLflow_Live_Sink equals
   the running sum of deltas produced by `TimingResultsStrategy`,
   and the aggregate snapshot equals the sum of per-key snapshots.
6. THE unit test suite SHALL include a property-based test for the
   fanout queue backpressure: for any sequence of enqueue events
   against a queue of capacity `N >= 1`, the queue length stays
   in `[0, N]` and `_fanout_dropped_events` increments exactly
   once per dropped event.
7. THE unit test suite SHALL include a property-based test for the
   MLflow live-run reuse rule: the post-run exporter reuses the
   live `run_id` iff both `tracking_uri` AND `benchmark_id` in
   `MLflow_Metadata_File` match the current configuration.
8. THE test suite SHALL include one integration test that runs
   `aiperf profile` against the in-repo mock server with
   `--otel-url` pointed at a fake OTLP sink and asserts that at
   least one OTLP export request is received while records are
   still flowing (smoke-level coverage of Requirement 7.1).
9. THE test suite SHALL include one integration test that runs
   `aiperf profile --mlflow --mlflow-tracking-uri file://<tmp>`
   against the mock server, then asserts that the uploaded
   `mlflow_export.json` equals the final local copy byte-for-byte
   (regression coverage of Requirement 7.3).
10. THE test suite SHALL include a unit test asserting that
    `--dashboard --mlflow-upload` fails with `ValueError` before
    any controller startup work is performed (regression coverage
    of Requirement 7.5).
11. All test JSON I/O introduced under `tests/**` SHALL use
    `orjson`, consistent with Requirement 8.5.

### Requirement 12: Local Verification Gates Before PR Submission

**User Story:** As an AIPerf maintainer, I want a defined set of local
commands that must all succeed before the takeover PR is submitted so that
review time is spent on substance, not on housekeeping.

#### Acceptance Criteria

1. `ruff format . && ruff check --fix .` SHALL succeed with no
   changes produced on a clean tree.
2. `uv run pytest tests/unit/ -n auto` SHALL succeed.
3. `uv run pytest -m component_integration -n auto` SHALL succeed
   for every test affected by this change (at minimum, exporters,
   records, and post_processors component integration tests).
4. `make validate-plugin-schemas` SHALL succeed.
5. `make check-agent-files-sync` SHALL succeed.
6. `make generate-all-docs` SHALL produce no diff on a clean tree
   (i.e., the committed CLI/env var docs are already current).
7. `make generate-all-plugin-files` SHALL produce no diff on a
   clean tree.
8. `pre-commit run --all-files` SHALL succeed.

### Requirement 14: OTel GenAI Semantic Convention Compliance

**User Story:** As an AIPerf operator using Grafana, Datadog, or any OTel
GenAI-aware observability backend, I want the metric stream from
`aiperf profile --otel-url ...` to be recognised by the vendor's
out-of-the-box GenAI dashboards so that I don't have to write custom
translations just to see AIPerf numbers.

**Rationale:** PR 656 head `628162da` emits metric names under an
`aiperf.*` private namespace with nanosecond units and without the
attributes required by the
[OTel GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-metrics/).
Maintainer feedback from Anthony Casagrande (NVIDIA) flagged that this
causes AIPerf telemetry to appear as opaque custom metrics in any
conformant OTel backend. Compliance is a merge-blocker, not a nit.
The spec is Development status at time of writing; we target its
current shape and note in documentation that the mapping will track
future revisions.

**Scope note:** Only the `OTel` streaming path is subject to this
requirement. MLflow live metrics already use their own `live.*`
namespace (per Requirements 3 and 7.2) and are not reshaped by this
requirement.

#### Acceptance Criteria

1. WHERE an AIPerf metric has a direct equivalent in the OTel GenAI
   client-metrics spec, THE `OTelMetricsResultsProcessor` SHALL emit
   the metric using the spec-defined name and unit:

   | AIPerf source | GenAI spec metric | Unit | Instrument |
   | --- | --- | --- | --- |
   | `request_latency` | `gen_ai.client.operation.duration` | `s` | Histogram |
   | `time_to_first_token` | `gen_ai.client.operation.time_to_first_chunk` | `s` | Histogram |
   | `inter_token_latency` | `gen_ai.client.operation.time_per_output_chunk` | `s` | Histogram |
   | `input_token_count` + `output_token_count` (merged) | `gen_ai.client.token.usage` with `gen_ai.token.type=input\|output` attribute | `{token}` | Histogram |

2. WHERE an AIPerf value is recorded in nanoseconds internally, THE
   mapping layer SHALL convert to seconds (float) before emitting,
   because all OTel GenAI duration metrics are spec'd in seconds.
3. THE histogram instruments for spec-named metrics SHALL be created
   with the `ExplicitBucketBoundaries` specified by the OTel GenAI
   spec for that metric, passed via
   `explicit_bucket_boundaries_advisory` on instrument creation.
4. Every emitted spec-named metric SHALL carry the Required attributes
   `gen_ai.operation.name` (mapped from `endpoint.type`, e.g.
   `chat` / `text_completion` / `embeddings`), `gen_ai.provider.name`
   (see 14.5), and, when available, `gen_ai.request.model` (from
   `endpoint.model_names[0]`).
5. THE AIPerf SHALL populate `gen_ai.provider.name` as follows:
   - (a) If `--gen-ai-provider <value>` is set, use that value verbatim.
   - (b) Otherwise, attempt auto-inference from the URL host of
     `endpoint.urls[0]` against a small, documented mapping table
     (e.g. `api.openai.com` → `openai`, `api.anthropic.com` → `anthropic`,
     `bedrock-runtime.*.amazonaws.com` → `aws.bedrock`,
     `generativelanguage.googleapis.com` → `gcp.gemini`,
     `*.vertex*.google*` → `gcp.vertex_ai`).
   - (c) Otherwise, emit the literal string `_OTHER` per the GenAI
     spec's fallback convention.
6. Every emitted spec-named metric SHALL also carry Recommended
   `server.address` and `server.port` attributes parsed from
   `endpoint.urls[0]`, when parseable.
7. WHEN a record carries an error, THE mapping layer SHALL populate
   the `error.type` attribute with a low-cardinality classifier
   derived from the error category (e.g. `timeout`, `http_5xx`,
   `parse_error`, `cancelled`); falling back to `_OTHER` when no
   classifier matches. THE AIPerf SHALL document the classifier set
   in the mapping module docstring.
8. AIPerf metrics that have NO equivalent in the GenAI spec (notably
   every metric derived from `CreditPhaseStats`) SHALL retain the
   `aiperf.*` namespace and SHALL NOT be renamed by this requirement.
   Such metrics MAY carry the same GenAI Required attributes
   (`gen_ai.operation.name`, `gen_ai.provider.name`,
   `gen_ai.request.model`) to enable cross-metric joins in dashboards.
9. AIPerf-specific dimensions (e.g. `aiperf.benchmark_phase`,
   `aiperf.session_num`, `aiperf.turn_index`, `aiperf.worker_id`,
   `aiperf.record_processor_id`) MAY be attached as additional
   attributes to spec-named metrics where useful; they SHALL keep
   the `aiperf.*` attribute prefix to avoid future spec collisions.
10. THE AIPerf SHALL NOT emit any metric under the `gen_ai.server.*`
    namespace. AIPerf is a client instrumentation per OTel GenAI
    semconv §"Generative AI client metrics"; the `gen_ai.server.*`
    family describes metrics reported by the model server itself
    and is out of scope for a benchmarking client.
11. THE AIPerf SHALL NOT emit `gen_ai.input.messages`,
    `gen_ai.output.messages`, `gen_ai.system_instructions`, or
    `gen_ai.tool.definitions` events. Content capture is an
    Opt-In concern per OTel GenAI semconv §Events and is explicitly
    deferred to a future PR (see Requirement 13.1 carve-out).
12. THE AIPerf SHALL remove the legacy `aiperf.*` metric names for
    the four metrics listed in 14.1. A migration note SHALL appear
    in `docs/tutorials/otel-mlflow.md` §Troubleshooting and in the
    takeover PR description so users currently reading
    `aiperf.request_latency_ns` know to switch to
    `gen_ai.client.operation.duration` (in seconds).
13. THE new mapping module SHALL live at
    `src/aiperf/post_processors/strategies/genai_semconv.py` and
    SHALL expose three tables (`METRIC_NAME_MAP`, `UNIT_CONVERTERS`,
    `ATTRIBUTE_BUILDERS`) plus a helper `infer_provider_name(...)`.
    Changes to the spec over time SHALL be absorbed by editing these
    tables; no other module should hard-code spec names.
14. THE `--gen-ai-provider` CLI option SHALL be added to
    `UserConfig`. Because Requirement 13.1 forbids new CLI options
    beyond PR 656 head, R14.14 formally carves out an exception for
    this one option. No other new CLI options are in scope.

### Requirement 13: Out of Scope (Explicit Non-Goals)

**User Story:** As an AIPerf maintainer, I want an explicit list of things
this takeover will NOT change so that scope stays small and review stays
focused.

#### Acceptance Criteria

1. THE takeover SHALL NOT add any new CLI option, env var, plugin,
   message type, or service beyond what is already in PR 656 head
   `628162da`, EXCEPT for the single `--gen-ai-provider` option
   carved out by Requirement 14.14.
2. THE takeover SHALL NOT remove or rename any public API that
   exists on `main` prior to this change.
3. THE takeover SHALL NOT switch the project away from
   `multiprocessing.Queue` to an async/ZMQ-based fanout transport;
   that redesign is explicitly deferred.
4. THE takeover SHALL NOT introduce a Prometheus exporter, a Datadog
   exporter, or any additional telemetry back-end; only OTel and
   MLflow are in scope.
5. THE takeover SHALL NOT modify the existing metrics-reference
   metric definitions or formulas; it only adds the
   `aiperf.timing.*` namespace and the GenAI semconv mapping
   (Requirement 14), and documents both.
6. THE takeover SHALL NOT implement OTel GenAI content capture
   (`gen_ai.input.messages`, `gen_ai.output.messages`,
   `gen_ai.system_instructions`, `gen_ai.tool.definitions`). That
   Opt-In event surface is deferred to a follow-up PR because it
   requires a separate design for truncation, redaction, and
   volume controls.


---

## TODO Checklist (Newcomer-Friendly Summary)

This is a non-normative cheat-sheet derived from the requirements above.
Each item links back to the requirement and, where possible, to the source
file or PR URL so a newcomer can locate the work.

### Feature parity with PR 656

- [ ] Live OTel metrics streaming wired via `--otel-url` (Req 1).
- [ ] Fanout_Process with bounded queue + drop-oldest (Req 2).
- [ ] Live MLflow metrics sink (Req 3).
- [ ] Deferred MLflow artifact upload from `ExporterManager` (Req 4).
- [ ] Strategy-protocol results processors (Req 5).
- [ ] `aiperf plot --mlflow-upload` (Req 6).

### Defect fixes

- [ ] Fix MLflow live flush starvation — time- and count-based flush
      inside `_flush_mlflow_metrics`
      (`src/aiperf/post_processors/otel_streaming_fanout.py`)
      (Req 7.1).
- [ ] Fix timing gauges logged as deltas — record cumulative
      per-`(metric, attribute_key)` snapshots to MLflow while OTel
      keeps deltas
      (`src/aiperf/post_processors/strategies/timing_results.py` +
      fanout sink) (Req 7.2).
- [ ] Fix stale uploaded `mlflow_export.json` — write final metadata
      before uploading (Req 7.3).
- [ ] Wire `AIPERF_OTEL_MAX_BUFFERED_RECORDS` to queue `maxsize`
      (Req 7.4).
- [ ] Reject `--dashboard --mlflow-upload` before `controller.run()`
      (Req 7.5).

### Round-3 style fixes

- [ ] Type annotation `list[str] | tuple[str, ...]` on
      `mlflow_resolved_artifact_globs` (Req 8.1).
- [ ] Remove `# Flushes any buffered data` comment in
      `records_manager.py` (Req 8.2).
- [ ] `install_optional_dependency_hint` leads with
      `pip install aiperf[...]` (Req 8.3).
- [ ] No callers of old `otel_streaming_enabled` property (Req 8.4).
- [ ] `orjson` in all new JSON I/O (Req 8.5).
- [ ] `X | Y`, `Field(description=...)`, enum-without-`.value`
      throughout new code (Req 8.6).

### GenAI semconv compliance (NEW)

- [ ] Add `strategies/genai_semconv.py` with three mapping tables
      (`METRIC_NAME_MAP`, `UNIT_CONVERTERS`, `ATTRIBUTE_BUILDERS`)
      plus `infer_provider_name()` (Req 14.13).
- [ ] Rename four metrics to `gen_ai.client.*` names (Req 14.1)
      and drop the `_ns` suffix after converting ns → s (Req 14.2).
- [ ] Merge `input_token_count` + `output_token_count` into single
      `gen_ai.client.token.usage` with `gen_ai.token.type` attribute
      (Req 14.1 row 4).
- [ ] Create histograms with `explicit_bucket_boundaries_advisory`
      per spec (Req 14.3).
- [ ] Build `gen_ai.operation.name`, `gen_ai.provider.name`,
      `gen_ai.request.model`, `server.address`, `server.port`,
      `error.type` attributes on every spec-named metric
      (Req 14.4, 14.6, 14.7).
- [ ] Add `--gen-ai-provider` CLI override (Req 14.5, 14.14).
- [ ] Keep `aiperf.timing.*` metrics; attach GenAI Required
      attributes to them for cross-metric joins (Req 14.8, 14.9).
- [ ] Verify NO `gen_ai.server.*` metric is emitted (Req 14.10).
- [ ] Verify NO GenAI event (`gen_ai.input.messages` etc.) is
      emitted — content capture is deferred (Req 14.11, 13.6).
- [ ] Migration note in tutorial + PR description listing the four
      renamed metrics (Req 14.12).

### Open inline review items

- [ ] `EndpointConfig.model_names` gets `min_length=1` (Req 1.8).
- [ ] Remove/consolidate any duplication between
      `ExporterManager.export_data` and `export_mlflow()` helper
      ([CodeRabbit review 2867380366](https://github.com/ai-dynamo/aiperf/pull/656#pullrequestreview-2867380366))
      (Req 4.7).

### Rebase + branch hygiene

- [ ] Rebase onto current `main` tip; resolve conflicts in
      `user_config.py`, `records_manager.py`, `exporter_manager.py`,
      `plugins.yaml` (Req 9.1, 9.2).
- [ ] Squash to Conventional-Commit messages with
      `Co-authored-by: Emmanuel Bashorun <bashorun.emma@gmail.com>`
      and `-s` (Req 9.3, 9.4).
- [ ] Push to `<username>/otel-mlflow-tracking` or update PR 656 in
      place via `maintainer_can_modify` (Req 9.5).

### Docs

- [ ] `make generate-all-docs` (Req 10.1, 10.2).
- [ ] `make generate-all-plugin-files` + `make validate-plugin-schemas`
      (Req 10.3).
- [ ] Architecture doc update (Req 10.4).
- [ ] Patterns doc update (Req 10.5).
- [ ] Metrics reference `aiperf.timing.*` (Req 10.6).
- [ ] New tutorial `docs/tutorials/otel-mlflow.md` (Req 10.7).
- [ ] `README.md` tutorial index + `docs/index.yml` entry (Req 10.8).
- [ ] `make check-agent-files-sync` green (Req 10.9).
- [ ] README mentions new extras (Req 10.10).

### Tests

- [ ] Property tests: `_normalize_otel_metrics_url`,
      `coerce_metric_values`, timing delta accounting, timing
      gauge snapshot, fanout backpressure, MLflow live-run reuse
      (Req 11.2–11.7).
- [ ] Integration test: live OTel export while records flow
      (Req 11.8).
- [ ] Integration test: uploaded `mlflow_export.json` equals final
      local copy (Req 11.9).
- [ ] Unit test: `--dashboard --mlflow-upload` rejected pre-run
      (Req 11.10).

### Pre-PR verification gates

- [ ] `ruff format . && ruff check --fix .` clean (Req 12.1).
- [ ] `uv run pytest tests/unit/ -n auto` passes (Req 12.2).
- [ ] `uv run pytest -m component_integration -n auto` passes for
      affected suites (Req 12.3).
- [ ] `make validate-plugin-schemas` passes (Req 12.4).
- [ ] `make check-agent-files-sync` passes (Req 12.5).
- [ ] `make generate-all-docs` produces no diff (Req 12.6).
- [ ] `make generate-all-plugin-files` produces no diff (Req 12.7).
- [ ] `pre-commit run --all-files` passes (Req 12.8).
