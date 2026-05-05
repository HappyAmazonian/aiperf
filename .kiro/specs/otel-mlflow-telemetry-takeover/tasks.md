<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Implementation Plan — OTel + MLflow Telemetry Takeover

## Summary

This is a PR takeover of [ai-dynamo/aiperf#656](https://github.com/ai-dynamo/aiperf/pull/656)
(head `628162da`, ~5000 LOC), not a greenfield build. Most components are already
implemented on the PR branch and need only rebase onto current `main` plus the five
round-2 defect fixes, a handful of round-3 style fixes, eight property-based tests,
two integration tests, and a documentation pass. The end state is a single, clean,
DCO-signed PR that preserves Emmanuel Bashorun's authorship via `Co-authored-by` on
every commit.

## How to use this list

- Tasks are numbered hierarchically (e.g. `3.2`) so every task can be referenced
  unambiguously. Top-level numbers (`3.`) are epics; decimals (`3.2`) are the
  subtasks that actually do work.
- Every task cites the requirements it satisfies and the design subsections it
  implements. The citation lives at the end of the task body: `Requirements: X.Y.
  Design: Components - <subsection>.`
- An asterisk after the checkbox (`- [ ]*`) marks a task as optional — skipped by
  the "run all tasks" mode. Core implementation, defect fixes, property tests
  mandated by Req 11, and verification gates are not optional.
- Each epic has a short rationale paragraph before its subtasks. The rationale is
  prose, not a checkbox.
- Tasks phrased "Verify X on the rebased branch" describe work that is already
  present in PR 656 head `628162da` — the action is to confirm the existing
  implementation survived the rebase and matches the design. Tasks phrased "Apply
  the fix to Y" or "Implement Z" describe genuinely new work.
- File path references use the `src/aiperf/...` and `tests/...` layout from
  current `main`. Line ranges are approximate on `628162da`; they will shift after
  rebase and are included only as navigational hints.
- Property-based tests (§8 and §§3–5 where defect-specific) each validate exactly
  one property from `design.md` §Correctness Properties. The property number and
  the quoted invariant appear in the task body so the test author does not have
  to re-derive it.

## Assumptions

Defaults taken where the design left open questions. Revise these only if the
user says so explicitly; every subsequent task is written against the default.

- **PR strategy.** Default to opening a **new PR from
  `<current-user>/otel-mlflow-tracking` that closes PR #656** because push access
  on `briefgaming/aiperf:feat/otel-mlflow-tracking` is not confirmed. If push
  access is later confirmed via the PR's `maintainer_can_modify` flag, task 12.3
  switches to "update PR #656 in place" instead.
- **Tutorial file.** Single-file tutorial at `docs/tutorials/otel-mlflow.md`
  (not split into `otel-streaming.md` + `mlflow-integration.md`).
- **`docs/index.yml` coordination.** No known concurrent editor of
  `docs/index.yml`; the Fern index entry is added in the dedicated docs commit
  (task 10.8). If a conflict surfaces at rebase time, resolve by keeping both
  entries.
- **Commit `sim:` footer.** Omit the `sim:` footer from every commit. This is
  upstream GitHub (ai-dynamo/aiperf), not an Amazon-internal repo, so the
  `sim:` convention does not apply.
- **Counter reset semantics (Property 3).** The property generator **includes**
  resets (phase transitions emitting a new cumulative from 0). Emitted delta on
  reset equals the new cumulative; no negative deltas are ever emitted.
- **Fanout spawn method.** Keep the existing daemon workaround
  (`mp.current_process().daemon = False` / restore) on the rebased branch. Do
  NOT pin `multiprocessing.get_context("spawn")` — that is a separate concern
  and would blow scope on this PR.
- **`mlflow_export.json` equality.** Start strict (bit-for-bit) in Property 8
  and the integration test at task 5.4. If the integration test at task 9.2
  reveals that the MLflow backend rewrites line endings or trailing bytes on
  `log_artifacts`, fall back to `orjson.loads` semantic equality and document
  the fallback in a regression-test comment.

---

## Tasks

- [ ] 1. Branch setup and initial rebase

  Get PR 656's code onto a local branch rebased on current `main` before any
  other work begins. Everything downstream assumes the rebased tree is the
  working copy.

  - [ ] 1.1 Add `briefgaming/aiperf` as a git remote and fetch branch
        `feat/otel-mlflow-tracking` at SHA `628162da`
    - Add the fork as remote `briefgaming`:
      `git remote add briefgaming https://github.com/briefgaming/aiperf.git`
    - Fetch the feature branch: `git -P fetch briefgaming feat/otel-mlflow-tracking`
    - Confirm the SHA matches: `git -P log -n 1 briefgaming/feat/otel-mlflow-tracking`
      should print `628162da`.
    - Requirements: 9.1. Design: Rebase Strategy step 1.

  - [ ] 1.2 Create the takeover branch `<current-user>/otel-mlflow-tracking`
        from current `main` HEAD
    - `git -P fetch origin main`
    - `git checkout -b <current-user>/otel-mlflow-tracking origin/main`
    - Record the current `main` SHA in a scratch note; the requirements doc
      references `5b04befe` as a sample, but the actual target is the `main`
      HEAD at the moment work begins.
    - Requirements: 9.1, 9.5. Design: Rebase Strategy step 2.

  - [ ] 1.3 Cherry-pick PR 656's five commits onto the takeover branch and
        resolve the conflict in `src/aiperf/common/config/user_config.py`
    - Apply the PR 656 commits in order from
      `briefgaming/feat/otel-mlflow-tracking`.
    - On conflict in `user_config.py`: keep all of `main`'s existing
      `EndpointConfig` / `UserConfig` fields and also apply PR 656's additions
      (`--otel-url`, `--stream`, `--mlflow*`, `--mlflow-upload`). Re-import
      `_normalize_otel_metrics_url`. Leave the `min_length=1` work for task 2.6
      (it is a separate concern).
    - Requirements: 9.2. Design: Rebase Strategy step 3 (conflict table row 1)
      and Components - `common/config/user_config.py`.

  - [ ] 1.4 Resolve the conflict in `src/aiperf/records/records_manager.py`
    - Keep `main`'s handler changes since `8763c57c`. Add PR 656's
      `_timing_results_processors` list and the `@on_message` handlers that
      dispatch `CREDIT_PHASE_START`, `CREDIT_PHASE_PROGRESS`,
      `CREDIT_PHASE_SENDING_COMPLETE`, `CREDIT_PHASE_COMPLETE` into those
      processors.
    - Also keep the `_flush_metric_results_processors(force=True)` call in
      `_process_results`. The stale-comment removal is task 2.2; do not delete
      the comment here yet.
    - Requirements: 9.2. Design: Rebase Strategy step 3 (conflict table row 2)
      and Components - `records/records_manager.py`.

  - [ ] 1.5 Resolve the conflict in `src/aiperf/exporters/exporter_manager.py`
    - Keep `main`'s export orchestration. Add PR 656's `deferred_exporters`
      list and the call site that runs `MLflowDataExporter` after the local
      exporters finish.
    - If the PR 656 head contains a free-function `export_mlflow()` alongside
      the exporter class, note it for collapsing in task 5.2 but do not delete
      it yet (keep the rebase atomic).
    - Requirements: 9.2. Design: Rebase Strategy step 3 (conflict table row 3)
      and Components - `exporters/exporter_manager.py`.

  - [ ] 1.6 Resolve the conflict in `src/aiperf/plugin/plugins.yaml`
    - Keep `main`'s entries and add PR 656's two new entries:
      `results_processor.otel_metrics_streamer` and `data_exporter.mlflow`.
      Both entries are documented in design §Components - `plugin/plugins.yaml`.
    - Do not run the plugin regenerator yet; that happens in task 10.2.
    - Requirements: 9.2, 10.3. Design: Rebase Strategy step 3 (conflict table
      row 4) and Components - `plugin/plugins.yaml`.

  - [ ] 1.7 Initial build verification on the rebased branch
    - Run `make first-time-setup` (or `make install` if the environment is
      already initialised) and confirm the install succeeds with the new
      optional extras present in `pyproject.toml` (`aiperf[otel]`,
      `aiperf[mlflow]`).
    - Run `aiperf --help` and confirm the new flags appear without import
      errors. No OTel or MLflow SDK should be imported during `--help`
      dispatch (per Req 1.7); a failure here indicates a missing guard.
    - Run `uv run pytest tests/unit/ -n auto --collect-only` to confirm
      collection succeeds with the rebased tree (tests need not pass yet).
    - Requirements: 1.7, 10.10. Design: Components -
      `common/optional_dependencies.py` and `pyproject.toml`.

- [ ] 2. Round-3 style fixes (quick wins)

  These are trivial edits with a tiny blast radius. Land them before any
  defect fix so the diff for the defect fixes stays focused on behavior.

  - [ ] 2.1 Fix return-type annotation on `mlflow_resolved_artifact_globs`
    - In `src/aiperf/common/config/user_config.py`, change the return
      annotation of `mlflow_resolved_artifact_globs` from `tuple[str]` (an
      illegal single-element type) to `list[str] | tuple[str, ...]`.
    - Verify the runtime value unchanged: function body still returns the
      same object.
    - Requirements: 8.1. Design: Components - `common/config/user_config.py`.

  - [ ] 2.2 Remove stale `# Flushes any buffered data` comment in
        `records_manager.py`
    - In `src/aiperf/records/records_manager.py`, delete the line
      `# Flushes any buffered data` flagged in CodeRabbit round-3. The
      comment is a leftover that no longer describes the code it sits
      above.
    - Requirements: 8.2. Design: Components - `records/records_manager.py`.

  - [ ] 2.3 Verify `install_optional_dependency_hint` wording leads with
        `pip install aiperf[...]`
    - Open `src/aiperf/common/optional_dependencies.py`.
    - Confirm `install_optional_dependency_hint(extra)` returns a string whose
      first line is `pip install aiperf[<extra>]`. The `uv add ...` form, if
      present, is a secondary hint.
    - If the current order is reversed (PR 656 may still lead with `uv add`),
      swap them. Per maintainer `ajcasagrande`, the hint targets end-users,
      not aiperf contributors, so `pip` is primary.
    - Requirements: 8.3. Design: Components -
      `common/optional_dependencies.py`.

  - [ ] 2.4 Audit for stale `otel_streaming_enabled` references and rename to
        `otel_collector_enabled`
    - Run `grep -rn otel_streaming_enabled src/aiperf/` (or the agent's
      `grepSearch` tool). Expected result: zero hits after this task.
    - For every hit, rewrite the call site to use the preserved property name
      `otel_collector_enabled` on `UserConfig`. Do not introduce new callers
      of the old name anywhere.
    - Requirements: 8.4. Design: Components - `common/config/user_config.py`.

  - [ ] 2.5 Audit new code for stdlib `json` usage and convert to `orjson`
    - Scope: every file touched by PR 656 plus any new file added in this
      takeover. Typical hotspots: `exporters/mlflow_data_exporter.py`,
      `post_processors/otel_streaming_fanout.py`, `plot/cli_runner.py`.
    - For each stdlib `json.loads` / `json.dumps` call, replace with
      `orjson.loads(s)` / `orjson.dumps(d)` (remember `orjson.dumps` returns
      `bytes`; decode if a `str` is needed). Do not touch third-party schema
      loaders or pre-existing test fixtures that already live on `main`.
    - Requirements: 8.5, 8.6. Design: Data Models (MLflowMetadata uses
      `orjson`) and Components - `plot/cli_runner.py`.

  - [ ] 2.6 Add `min_length=1` to `EndpointConfig.model_names`
    - In `src/aiperf/common/config/user_config.py`, update the
      `EndpointConfig.model_names` `Field(...)` call to
      `Field(min_length=1, description=...)`.
    - Why: `OTelMetricsResultsProcessor._build_resource_attributes` indexes
      `model_names[0]`, which explodes with `IndexError` on an empty list.
      A Pydantic validator is the right gate.
    - Confirm no existing caller passes `[]`: `grep -n "model_names=\[\]"` in
      `src/aiperf/` and `tests/` should return zero hits.
    - Requirements: 1.8. Design: Components - `common/config/user_config.py`
      (Requirement 1.8 paragraph).

- [ ] 3. Round-2 defect 7.1 — MLflow live flush starvation

  On the rebased branch, `_flush_mlflow_metrics` only fires when the fanout
  queue goes idle. Under sustained load the queue never empties, so MLflow
  live scalars never flush. Fix: drive flushes off a monotonic clock plus a
  count trigger, not off queue-idle detection.

  - [ ] 3.1 Implement the monotonic-time flush driver in `_maybe_flush`
    - File: `src/aiperf/post_processors/otel_streaming_fanout.py`.
    - Add `last_flush_monotonic: float = time.monotonic()` to the fanout
      main loop's local state.
    - Rewrite `_maybe_flush(mlflow_state, *, now, force, config)` with the
      exact semantics from design §Components -
      `post_processors/otel_streaming_fanout.py` Requirement 7.1 section:
      return True (and perform the flush) when
      `force` OR `len(mlflow_state.buffer) >= config.max_batch_records` OR
      `(now - last_flush_monotonic) >= config.export_interval_seconds`.
      On flush, reset `last_flush_monotonic = now`.
    - In the main loop, call `_maybe_flush(..., now=time.monotonic(),
      force=False, ...)` after every dispatched event AND after every
      `queue.Empty` timeout on `event_queue.get(timeout=poll_timeout_sec)`.
    - Retain the existing count-based inline check in
      `_append_mlflow_metric` as a fast path, but it is no longer
      load-bearing.
    - Explicit `flush` and `shutdown` events still call `_maybe_flush(...,
      force=True)` unchanged.
    - Requirements: 7.1, 2.1, 2.3. Design: Components -
      `post_processors/otel_streaming_fanout.py` (Requirement 7.1 design —
      flush driver).

  - [ ] 3.2 Add property-based test P7 — flush trigger invariant
    - Create `tests/unit/post_processors/test_fanout_flush_trigger_property.py`.
    - Property 7 invariant (from design §Correctness Properties):
      "For any sequence of events dispatched into the fanout over a synthetic
      monotonic clock, the number of flush invocations over a window of
      duration W seconds is at least floor(W / config.export_interval_seconds),
      regardless of whether the queue was ever empty during the window. In
      addition, any time the buffer reaches config.max_batch_records entries,
      a flush is invoked before the next event is appended. These are the
      only two conditions that trigger an implicit flush; explicit `flush`
      and `shutdown` events always force a flush."
    - Source of truth: `_maybe_flush` in
      `post_processors/otel_streaming_fanout.py`.
    - Use Hypothesis with `@settings(max_examples=100)` and a synthetic
      monotonic clock (monkeypatch `time.monotonic`). Assert both the
      count-trigger and the time-trigger branches fire independently.
    - Tag comment at the top of the test:
      `# Feature: otel-mlflow-telemetry-takeover, Property 7: MLflow live flush is bounded by count AND monotonic time`.
    - Requirements: 7.1, 11.1. Design: Correctness Properties §Property 7
      and Testing Strategy §Unit + Property Test Matrix.

- [ ] 4. Round-2 defect 7.2 — MLflow gauges record cumulative snapshots

  On the rebased branch, `up_down_counter_add` events are forwarded to MLflow
  as raw deltas, producing oscillating values (`+1 / -1 / +1 / -1`) that
  misrepresent gauge-style metrics such as `in_flight_requests`. Fix: keep
  OTel receiving deltas (correct per OTel spec) but let the fanout process
  accumulate cumulative snapshots per `(name, attribute_key)` for MLflow.

  - [ ] 4.1 Introduce `mlflow_gauge_snapshots` state and `_attribute_key`
        helper in the fanout process
    - File: `src/aiperf/post_processors/otel_streaming_fanout.py`.
    - Add `mlflow_gauge_snapshots: dict[str, dict[AttributeKey, float]]` as
      a field on `MLflowFanoutState` (design §Data Models §Strategy state
      dicts).
    - Add `AttributeKey = tuple[tuple[str, str], ...]` type alias and the
      helper `_attribute_key(attrs)` with the canonicalisation from design
      §Components - `post_processors/otel_streaming_fanout.py`:
      `return tuple(sorted((str(k), str(v)) for k, v in (attrs or {}).items()))`.
    - Requirements: 7.2. Design: Components -
      `post_processors/otel_streaming_fanout.py` (Requirement 7.2 design)
      and Data Models §Strategy state dicts (fanout half).

  - [ ] 4.2 Split the `up_down_counter_add` event handler: OTel keeps delta,
        MLflow accumulates
    - File: `src/aiperf/post_processors/otel_streaming_fanout.py`, function
      `_dispatch_event`.
    - For `event["type"] == "up_down_counter_add"`:
      1. OTel branch: `up_down_counter.add(delta, attributes=attrs)` —
         unchanged. Delta temporality is correct for OTel.
      2. MLflow branch: compute `key = _attribute_key(attrs)`; get
         `snapshots = mlflow_gauge_snapshots.setdefault(name, {})`; set
         `snapshots[key] = snapshots.get(key, 0.0) + delta`. If
         `abs(snapshots[key]) < 1e-9`, `del snapshots[key]` to bound
         memory. Otherwise log `live.<name>` to MLflow with the cumulative
         `snapshots[key]`.
    - For `event["type"] == "counter_add"` (monotonic counter):
      - OTel branch: `counter.add(delta, ...)` — unchanged.
      - MLflow branch: log the delta as-is (counters are already
        non-negative deltas per Property 3; cumulative accumulation is not
        needed on the MLflow side for these).
    - Requirements: 7.2, 3.1. Design: Components -
      `post_processors/otel_streaming_fanout.py` (Requirement 7.2 design).

  - [ ] 4.3 Add property-based test P4 — MLflow gauge snapshot invariant
    - Create
      `tests/unit/post_processors/test_mlflow_gauge_snapshot_property.py`.
    - Property 4 invariant (from design §Correctness Properties):
      "For any finite sequence of `up_down_counter_add` events
      `(name_i, delta_i, attrs_i)` consumed by the fanout gauge sink, at
      every intermediate step k and for every `(name, attr_key)` pair, the
      value `mlflow_gauge_snapshots[name][attr_key]` (or 0.0 if evicted
      for |value| < 1e-9) equals `sum(delta_i for i <= k such that
      name_i == name AND _attribute_key(attrs_i) == attr_key)`.
      Additionally, the aggregate snapshot per name (sum of values across
      all attribute keys, treating evicted keys as 0) equals the running
      sum of all deltas for that name."
    - Source of truth: gauge branch of `_dispatch_event` in
      `post_processors/otel_streaming_fanout.py`.
    - Use the in-memory MLflow fixture from
      `tests/unit/exporters/conftest.py` (created in task 8.0) or construct
      a local stub. Hypothesis strategy: sequences of `(name, delta, attrs)`
      drawn from a small discrete space of names and attribute dicts so the
      multi-key case is exercised.
    - Tag comment:
      `# Feature: otel-mlflow-telemetry-takeover, Property 4: MLflow gauge snapshot equals cumulative sum of deltas per attribute key`.
    - Requirements: 7.2, 11.5. Design: Correctness Properties §Property 4.

- [ ] 5. Round-2 defect 7.3 — MLflow metadata order-of-operations

  On the rebased branch, `MLflowDataExporter._export_sync` uploads
  `mlflow_export.json` (via `mlflow.log_artifacts(output_dir)`) before
  rewriting it with the final `uploaded_artifacts` / `reused_live_run`
  fields. The result: MLflow stores a stale copy that does not match the
  local file. Fix: reorder so the final metadata is written to disk first
  and uploaded in the same `log_artifacts` pass.

  - [ ] 5.1 Reorder `MLflowDataExporter._export_sync` into the 9-step
        sequence
    - File: `src/aiperf/exporters/mlflow_data_exporter.py`, method
      `_export_sync`.
    - Implement the exact ordering from design §Components -
      `exporters/mlflow_data_exporter.py` (Requirement 7.3 design):
      1. Load existing `mlflow_export.json` via `orjson.loads`.
      2. Decide reuse: `metadata.tracking_uri == config.tracking_uri AND
         metadata.benchmark_id == benchmark_id`.
      3. Resolve `run_context` (reuse `run_id` vs new run).
      4. Open the MLflow run.
      5. `log_batch` metrics / params / tags.
      6. Enumerate artifact files via `mlflow_resolved_artifact_globs`,
         **excluding** `mlflow_export.json`.
      7. Compute
         `uploaded_artifact_names = [p.name for p in artifacts] + ["mlflow_export.json"]`.
      8. Write final `mlflow_export.json` to disk with the final
         `uploaded_artifacts` list + `reused_live_run` flag, via
         `orjson.dumps`.
      9. `mlflow.log_artifacts(output_dir)` in a **single** pass so the
         uploaded bytes match disk. Close the run.
    - The invariant established by steps 8 -> 9: `Path(output_dir /
      "mlflow_export.json").read_bytes()` at step 9 start equals the bytes
      MLflow stores.
    - Requirements: 7.3, 4.5, 4.6. Design: Components -
      `exporters/mlflow_data_exporter.py` (nine-step ordering).

  - [ ] 5.2 Collapse any duplicate `export_mlflow()` helper into the
        exporter
    - Audit `src/aiperf/exporters/exporter_manager.py` and surrounding
      modules for a free-function `export_mlflow()` or similar helper
      flagged by CodeRabbit review 2867380366.
    - If found, move its content into `MLflowDataExporter._export_sync`
      and invoke the exporter through `ExporterManager.deferred_exporters`
      only. The exporter must be the single code path.
    - If no duplicate is found after the rebase, leave this task
      complete (it was already collapsed upstream) and note the audit in
      the commit message.
    - Requirements: 4.7. Design: Components -
      `exporters/mlflow_data_exporter.py` (Requirement 4.7 — deduplication)
      and `exporters/exporter_manager.py`.

  - [ ] 5.3 Add property-based test P8 — metadata byte-equality invariant
    - Create
      `tests/unit/exporters/test_mlflow_metadata_equality_property.py`.
    - Property 8 invariant (from design §Correctness Properties):
      "For any completed run of `MLflowDataExporter._export_sync`, the
      bytes that the mocked `mlflow.log_artifacts` call receives for
      `mlflow_export.json` equal `Path(output_dir / "mlflow_export.json").read_bytes()`
      at the moment `_export_sync` returns. Additionally, the observed
      call order on the MLflow client is:
      `[log_batch | log_params | set_tags]* -> file-system write -> log_artifacts(output_dir)`,
      with `log_artifacts` called exactly once."
    - Source of truth: `MLflowDataExporter._export_sync`.
    - Use the in-memory MLflow client fixture (task 8.0) that captures
      call order. Hypothesis strategy: varying numbers of
      metrics/params/tags and varying artifact glob matches.
    - Tag comment:
      `# Feature: otel-mlflow-telemetry-takeover, Property 8: Uploaded mlflow_export.json equals final local copy byte-for-byte`.
    - Requirements: 7.3, 4.5, 4.6, 11.1. Design: Correctness Properties
      §Property 8.

  - [ ] 5.4 Add integration test — metadata round-trip
    - Create
      `tests/integration/exporters/test_mlflow_metadata_roundtrip.py`.
    - Test: run `aiperf profile --mlflow --mlflow-tracking-uri file://<tmp>`
      against the in-repo mock server with a small concurrency (e.g. 2)
      and short request count (e.g. 5). After the run, read the local
      `mlflow_export.json` and read the uploaded copy from the MLflow
      tracking store (the `file://` backend writes to disk under
      `<tmp>/0/<run_id>/artifacts/mlflow_export.json`). Assert the two
      byte strings are equal.
    - If the assertion fails because of line-ending or trailing-byte
      normalization by the `file://` backend, fall back per §Assumptions
      to `orjson.loads` semantic equality and add a comment explaining
      the fallback.
    - Mark the test with `@pytest.mark.component_integration` so it runs
      under `uv run pytest -m component_integration`.
    - Requirements: 11.9, 7.3. Design: Testing Strategy §Integration
      Tests (row 2).

- [ ] 6. Round-2 defect 7.4 — Queue maxsize env var wiring

  On the rebased branch, `multiprocessing.Queue(maxsize=...)` may still be
  hard-coded. The env var `AIPERF_OTEL_MAX_BUFFERED_RECORDS` was intended to
  control it. Fix: verify the environment value actually reaches the Queue
  constructor, and add a regression test.

  - [ ] 6.1 Verify `AIPERF_OTEL_MAX_BUFFERED_RECORDS` flows into
        `Queue(maxsize=...)`
    - File: `src/aiperf/post_processors/otel_metrics_results_processor.py`,
      method `_start_fanout_process`.
    - Confirm the processor reads
      `Environment.OTEL.MAX_BUFFERED_RECORDS` (design §Components -
      `common/environment.py`) into `self._fanout_queue_maxsize` and passes
      that value to `context.Queue(maxsize=self._fanout_queue_maxsize)`.
    - If the current code hard-codes the value, replace the literal with
      the environment field. Do not introduce a second default; the default
      lives on `_OTelSettings.MAX_BUFFERED_RECORDS` in
      `common/environment.py`.
    - Requirements: 7.4, 2.1. Design: Components -
      `post_processors/otel_metrics_results_processor.py` (Design notes
      first bullet).

  - [ ] 6.2 Add regression test — queue-config smoke
    - Create `tests/unit/post_processors/test_fanout_queue_config.py`.
    - Test: set `AIPERF_OTEL_MAX_BUFFERED_RECORDS=1` via
      `monkeypatch.setenv`, construct `OTelMetricsResultsProcessor`, call
      `_start_fanout_process`, and assert the processor's queue satisfies
      `queue._maxsize == 1`. Tear down the fanout process cleanly.
    - This is an example test (not property-based). Naming per AGENTS.md:
      `test_fanout_queue_maxsize_reads_env_var`.
    - Requirements: 7.4, 11.1, 2.1. Design: Testing Strategy §Unit +
      Property Test Matrix (`test_fanout_queue_maxsize_env` row).

- [ ] 7. Round-2 defect 7.5 — `--dashboard --mlflow-upload` early rejection

  On the rebased branch, the mutual-exclusion check between `--dashboard`
  and `--mlflow-upload` fires after `PlotController` has already started its
  blocking HTTP server. Fix: move the check to the very top of
  `run_plot_controller` so the error surfaces before any startup work.

  - [ ] 7.1 Move the mutual-exclusion check to step 3 of
        `run_plot_controller`
    - File: `src/aiperf/plot/cli_runner.py`, function
      `run_plot_controller`.
    - The function should execute in the order listed in design §Components
      - `plot/cli_runner.py` (Public surface): (1) resolve paths, (2)
      coerce mode/theme, (3) if `user_config.dashboard AND
      user_config.mlflow_upload` raise `ValueError("--dashboard and
      --mlflow-upload are mutually exclusive")`, (4) resolve MLflow upload
      target if requested, (5) construct `PlotController`, (6) run.
    - Mirror the ordering in `src/aiperf/cli_commands/plot.py` argument
      coercion so invalid combinations fail at CLI parse when possible.
    - Requirements: 7.5, 6.1. Design: Components - `plot/cli_runner.py`
      (Requirement 7.5 design).

  - [ ] 7.2 Add regression test — dashboard/mlflow-upload rejection
    - Create or extend
      `tests/unit/plot/test_plot_cli_runner.py`.
    - Test: construct a `UserConfig` with `dashboard=True` and
      `mlflow_upload=True`, call `run_plot_controller(user_config)`, assert
      it raises `ValueError` whose message contains `mutually exclusive`.
      Verify `PlotController` was not constructed by patching its
      constructor and asserting `mock_plot_controller.assert_not_called()`.
    - Naming: `test_plot_dashboard_and_mlflow_upload_rejected_before_startup`.
    - Requirements: 7.5, 11.10. Design: Testing Strategy §Unit + Property
      Test Matrix (`test_plot_dashboard_and_mlflow_upload_rejected` row).

- [ ] 8. Preserved-behavior property-based tests

  These five property tests cover code that was already implemented in PR
  656 (not new defect-fix code). They exist to lock in the invariants so
  future refactors cannot silently regress them. Each test validates
  exactly one property from design §Correctness Properties.

  - [ ] 8.0 Add shared fixtures for OTel and MLflow property tests
    - Create `tests/unit/post_processors/conftest.py` with a fake OTel
      scaffold: substitutes `MeterProvider`, `OTLPMetricExporter`, and
      `PeriodicExportingMetricReader`, and records every `record()` /
      `add()` call in-memory. Used by Properties 3, 4, 5, 7.
    - Create `tests/unit/exporters/conftest.py` with an in-memory MLflow
      client: captures `log_metric`, `log_params`, `set_tags`, `log_batch`,
      `log_artifacts` with call order preserved. Used by Properties 6, 8.
    - Fixtures reset between tests via the auto-singleton-reset mechanism
      in `tests/conftest.py` (per AGENTS.md §Testing Conventions).
    - Requirements: 11.1. Design: Testing Strategy §Fixtures Needed.

  - [ ] 8.1 Add property-based test P1 — OTel URL normalization
    - Create
      `tests/unit/common/config/test_user_config_otel_url.py`.
    - Property 1 invariant (from design §Correctness Properties):
      "For any input u drawn from the set of bare hosts, `host:port`
      strings, `http(s)://host[:port][/path]` URLs, the result of
      `_normalize_otel_metrics_url(u)` ends with the path segment
      `/v1/metrics` exactly once AND
      `urlparse(result).hostname == urlparse_input.hostname` AND
      `urlparse(result).port == urlparse_input.port` AND
      `_normalize_otel_metrics_url(_normalize_otel_metrics_url(u)) == _normalize_otel_metrics_url(u)`.
      For any input whose scheme is not in {http, https} (e.g. file, ftp,
      grpc, ws, tcp), `_normalize_otel_metrics_url` raises a configuration
      error before Fanout_Process is spawned."
    - Source of truth: `user_config._normalize_otel_metrics_url`.
    - Hypothesis strategies: compose from `st.sampled_from` host names
      plus optional `:port` plus optional `http(s)://` prefix plus
      optional path; a separate strategy generates invalid-scheme inputs
      to test the rejection branch.
    - Tag comment:
      `# Feature: otel-mlflow-telemetry-takeover, Property 1: OTel URL normalization is host-preserving and idempotent`.
    - Requirements: 1.3, 1.4, 11.2. Design: Correctness Properties
      §Property 1.

  - [ ] 8.2 Add property-based test P2 — `coerce_metric_values`
    - Create
      `tests/unit/post_processors/test_coerce_metric_values_property.py`.
    - Property 2 invariant:
      "For any input value of type `int | float | bool | list[T] | T`,
      where T ranges over numeric and non-numeric scalars,
      `coerce_metric_values(value)` satisfies: booleans (including bool
      items inside lists) are dropped; a single numeric non-bool scalar
      produces a singleton list of floats; a mixed list retains exactly
      those entries that are numeric AND not bool; non-numeric non-bool
      scalars and containers (strings, dicts, tuples, None) produce `[]`."
    - Source of truth:
      `OTelMetricsResultsProcessor._coerce_metric_values` (or the
      module-level helper it calls).
    - Tag comment:
      `# Feature: otel-mlflow-telemetry-takeover, Property 2: coerce_metric_values keeps only numeric-non-boolean entries`.
    - Requirements: 1.6, 11.3. Design: Correctness Properties §Property 2.

  - [ ] 8.3 Add property-based test P3 — Timing counter delta accounting
    - Create
      `tests/unit/post_processors/test_timing_counter_delta_property.py`.
    - Property 3 invariant:
      "For any finite sequence of cumulative counter snapshots
      s_0, s_1, ..., s_n where each s_i >= 0 and resets are modelled as
      s_i < s_{i-1} for some i, the emitted deltas d_i = delta_fn(s_i)
      computed by `_timing_counter_state` satisfy: d_i >= 0 for every i;
      on reset, d_i == s_i (the new cumulative becomes the delta);
      otherwise d_i == s_i - last_observed_for(phase, metric);
      sum(d_0..d_n) == s_n when no resets occur; sum(d_0..d_n) >= s_n
      when resets occur (cumulative loss is counted as new deltas
      post-reset). All emitted counter events carry metric names with
      the prefix `aiperf.timing.`."
    - Source of truth: `OTelStrategyContextProtocol.counter_delta`
      implemented inside `OTelMetricsResultsProcessor`.
    - Per §Assumptions, the Hypothesis strategy **includes** reset
      sequences (elements where `s_i < s_{i-1}`).
    - Tag comment:
      `# Feature: otel-mlflow-telemetry-takeover, Property 3: Timing counter delta accounting is non-negative and sums to cumulative`.
    - Requirements: 5.3, 11.4. Design: Correctness Properties §Property 3.

  - [ ] 8.4 Add property-based test P5 — Fanout queue backpressure
    - Create
      `tests/unit/post_processors/test_fanout_backpressure_property.py`.
    - Property 5 invariant:
      "For any capacity N >= 1, for any sequence of E enqueue events
      dispatched against a queue that is never drained by a consumer, the
      observed queue length after processing each event lies in [0, N]
      AND `_fanout_dropped_events == max(0, E - N)` AND every drop
      corresponds to exactly one `get_nowait()` + `put_nowait()` retry
      pair (no doubled counting, no lost increments)."
    - Source of truth:
      `OTelMetricsResultsProcessor._enqueue_event` plus
      `_drop_oldest_fanout_event`.
    - Use a synchronous in-process queue stub so the test can drive the
      producer without a real consumer thread.
    - Tag comment:
      `# Feature: otel-mlflow-telemetry-takeover, Property 5: Fanout queue backpressure drops oldest and counts exactly`.
    - Requirements: 2.2, 7.4, 11.6. Design: Correctness Properties
      §Property 5.

  - [ ] 8.5 Add property-based test P6 — MLflow live-run reuse rule
    - Create
      `tests/unit/exporters/test_mlflow_live_run_reuse_property.py`.
    - Property 6 invariant:
      "For any pair of `MLflow_Metadata_File` and current `UserConfig`,
      the post-run exporter reuses the live `run_id` if and only if
      `metadata.tracking_uri == user_config.mlflow_tracking_uri` AND
      `metadata.benchmark_id == current_benchmark_id`. In the reuse case,
      `reused_live_run == True` in the final metadata; otherwise a new
      run is created and `reused_live_run == False`."
    - Source of truth: `MLflowDataExporter._resolve_run_context`.
    - Hypothesis strategy: draw `(metadata_tracking_uri,
      config_tracking_uri, metadata_benchmark_id, config_benchmark_id)`
      independently from a small URI/ID alphabet so the 4 combinations
      (match/mismatch x 2) are exercised.
    - Tag comment:
      `# Feature: otel-mlflow-telemetry-takeover, Property 6: MLflow live-run reuse rule`.
    - Requirements: 4.2, 11.7. Design: Correctness Properties §Property 6.

- [ ] 9. Integration tests

  Two end-to-end tests that run the real `aiperf profile` and `aiperf plot`
  paths against the in-repo mock server and a local MLflow `file://` store.
  They guard against regressions the unit tests cannot see.

  - [ ] 9.1 Live OTel export smoke test
    - Create
      `tests/integration/post_processors/test_otel_live_export.py`.
    - Test: start a fake OTLP HTTP sink (aiohttp test server) on a random
      port, run `aiperf profile --otel-url http://127.0.0.1:<port>` against
      the mock server with `--concurrency 4 --request-count 20
      --stream default`. Assert the fake sink receives at least one
      `POST /v1/metrics` while records are still flowing (not only at
      shutdown).
    - Why: this is the end-to-end regression coverage for Req 7.1 (flush
      starvation). A run that only flushes at shutdown would still receive
      one export and would pass a naive test; the assertion therefore
      checks the first export arrives before the run's declared completion.
    - Mark `@pytest.mark.component_integration`.
    - Requirements: 11.8, 7.1. Design: Testing Strategy §Integration Tests
      (row 1).

  - [ ] 9.2 Plot MLflow upload round-trip
    - Create
      `tests/integration/plot/test_plot_mlflow_upload.py`.
    - Test: run `aiperf profile` with `--mlflow --mlflow-tracking-uri
      file://<tmp>` then run `aiperf plot --input-dir <output_dir>
      --mlflow-upload` against the same `<tmp>`. Assert:
      (a) both commands use the same `run_id` (reuse rule from Property 6),
      (b) the MLflow run artifacts directory contains at least one plot
      file, (c) the plot upload does not rewrite `mlflow_export.json`.
    - Mark `@pytest.mark.component_integration`.
    - Requirements: 6.1, 6.2, 6.3, 11.9. Design: Testing Strategy
      §Integration Tests (row 3).

- [ ] 10. Documentation

  Docs land near the end so the code is stable when the prose describes
  it. Auto-generated docs come first because their regeneration is
  mechanical; hand-edited docs come next; the Four-File Sync check is the
  last docs step before verification gates.

  - [ ] 10.1 Regenerate CLI + env-var auto-docs
    - Run `make generate-all-docs`. This invokes `generate-cli-docs` and
      `generate-env-vars-docs` and rewrites `docs/cli-options.md` and
      `docs/environment-variables.md`.
    - Commit the regenerated files as part of the docs commit (task 10.9
      or a later rebase squash). Confirm the regenerated
      `docs/environment-variables.md` contains the four new
      `AIPERF_OTEL_*` env vars from Req 2.3.
    - Requirements: 10.1, 10.2. Design: Documentation Plan rows 1-2.

  - [ ] 10.2 Regenerate plugin artifact files
    - Run `make generate-all-plugin-files`. This regenerates the files
      under `src/aiperf/plugin/generated/` to reflect the two new
      `plugins.yaml` entries (`results_processor.otel_metrics_streamer`,
      `data_exporter.mlflow`).
    - Run `make validate-plugin-schemas` and confirm it exits clean.
    - Requirements: 10.3. Design: Documentation Plan row 3.

  - [ ] 10.3 Update `docs/architecture.md`
    - Add a new subsection "Telemetry Plane" that covers:
      (a) `OTelMetricsResultsProcessor` registration with `RecordsManager`,
      (b) the `multiprocessing.Queue` + Fanout_Process pattern,
      (c) strategy-protocol dispatch via `--stream`,
      (d) the Deferred_MLflow_Path in `ExporterManager`.
    - Reuse the two sequence diagrams from design §Architecture §Data
      Flow: Record Telemetry and §Data Flow: Timing Telemetry, and the
      flowchart from §Data Flow: Post-Run MLflow Artifact Upload.
    - Requirements: 10.4. Design: Documentation Plan row 4.

  - [ ] 10.4 Update `docs/dev/patterns.md`
    - Add a "Strategy Protocol Pattern" subsection with the
      `OTelResultsStrategyProtocol` interface and a short example of
      `MetricResultsStrategy` / `TimingResultsStrategy`.
    - Add a "Drop-Oldest Fanout Queue" subsection describing the backpressure
      semantics from design §Components -
      `post_processors/otel_metrics_results_processor.py`.
    - Requirements: 10.5. Design: Documentation Plan row 5.

  - [ ] 10.5 Update `docs/metrics-reference.md` with `aiperf.timing.*`
        namespace
    - Add a new section "Timing Namespace (`aiperf.timing.*`)" listing
      each counter and gauge emitted by `TimingResultsStrategy`, with
      columns: metric name, OTel instrument type (counter or
      up-down-counter), unit, description, source
      `CreditPhaseStats` field, applicable requirement number.
    - Do not modify existing metric definitions — Req 13.5 forbids that.
    - Requirements: 10.6, 13.5. Design: Documentation Plan row 6.

  - [ ] 10.6 Write new tutorial `docs/tutorials/otel-mlflow.md`
    - Follow the outline in design §Tutorial Outline
      (`docs/tutorials/otel-mlflow.md`): What you will learn,
      Prerequisites, Run a profile with telemetry enabled, Inspect live
      OTel data, Inspect live MLflow data, Post-run artifact upload,
      Attach plots, Troubleshooting.
    - Use the single-file name per §Assumptions.
    - Use mermaid diagrams only; no ASCII art (repo rule).
    - Requirements: 10.7. Design: Documentation Plan row 7 and
      Tutorial Outline.

  - [ ] 10.7 Update `README.md` — tutorial index and optional extras
    - Add a tutorial index entry "OTel + MLflow live telemetry" linking to
      `docs/tutorials/otel-mlflow.md`.
    - Mention the three optional extras in the installation section:
      `aiperf[mlflow]`, `aiperf[otel]`, and the composed
      `aiperf[mlflow,otel]`.
    - Requirements: 10.8, 10.10. Design: Documentation Plan row 8.

  - [ ] 10.8 Update `docs/index.yml` for Fern
    - Add an entry for `docs/tutorials/otel-mlflow.md` under the
      tutorials section of the Fern index.
    - Verify `tools/check_docs_index.py` exits clean.
    - Requirements: 10.8. Design: Documentation Plan row 9.

  - [ ] 10.9 Four-File Sync check
    - Touch AGENTS.md, CLAUDE.md, `.github/copilot-instructions.md`, or
      `.cursor/rules/python.mdc` only if a coding standard or build
      command truly changes (unlikely for this PR). Re-sync the four
      files byte-for-byte whenever one is edited.
    - Run `make check-agent-files-sync` and confirm it exits clean.
    - Requirements: 10.9. Design: Documentation Plan row 10.

- [ ] 11. Verification gates

  Every gate below must pass locally before the PR is pushed. These gates
  match the repo's pre-commit and CI checks; failing any one of them means
  the PR will not merge.

  - [ ] 11.1 `ruff format . && ruff check --fix .` is clean on a pristine
        tree
    - Run both commands. The second run must produce no diff (no files
      modified, no lint errors reported).
    - Requirements: 12.1. Design: Rebase Strategy step 10.

  - [ ] 11.2 `uv run pytest tests/unit/ -n auto` passes
    - The full unit-test suite must pass, including every property test
      and example test added in §3–§8.
    - No new `xfail` markers are allowed.
    - Requirements: 12.2, 11.1. Design: Rebase Strategy step 10.

  - [ ] 11.3 `uv run pytest -m component_integration -n auto` passes for
        affected suites
    - At minimum, the exporters, records, and post_processors component
      integration tests must pass. Both integration tests added in §9
      (`test_otel_live_export.py`, `test_plot_mlflow_upload.py`) are
      covered by this marker.
    - Requirements: 12.3. Design: Rebase Strategy step 10.

  - [ ] 11.4 `make validate-plugin-schemas` exits clean
    - Confirms the two new plugin entries added in task 1.6 pass schema
      validation.
    - Requirements: 12.4. Design: Rebase Strategy step 10.

  - [ ] 11.5 `make check-agent-files-sync` exits clean
    - Confirms the four agent-rule files are byte-identical. Already
      verified in task 10.9; re-run as a final gate.
    - Requirements: 12.5, 10.9. Design: Rebase Strategy step 10.

  - [ ] 11.6 `make generate-all-docs` produces no diff on a clean tree
    - Run the command and then `git -P status --porcelain`. Expected
      output: empty. A non-empty output means the CLI or env-var docs
      drifted since task 10.1; re-run 10.1 and re-check.
    - Requirements: 12.6. Design: Rebase Strategy step 10.

  - [ ] 11.7 `make generate-all-plugin-files` produces no diff on a
        clean tree
    - Same method as 11.6. A non-empty diff means a plugin artifact drifted
      since task 10.2; re-run 10.2 and re-check.
    - Requirements: 12.7. Design: Rebase Strategy step 10.

  - [ ] 11.8 `pre-commit run --all-files` exits clean
    - This is the aggregating gate; it invokes every hook including the
      generators, `validate-plugin-schemas`, `check-agent-files-sync`,
      `check-ergonomics`, `check-ruff-baselined`, `ruff`, and `ruff-format`.
    - Must succeed immediately on a clean tree (no "files modified by
      hook" retry loop).
    - Requirements: 12.8. Design: Rebase Strategy step 10.

- [ ] 12. PR submission

  Final commit structure, attribution, and PR creation. These steps
  only run after every gate in §11 is green.

  - [ ] 12.1 Squash to the final commit structure per Rebase Strategy
        step 7
    - Preferred structure: `feat(telemetry): live OTel metrics + MLflow
      export` followed by per-fix commits
      `fix(telemetry): flush MLflow live metrics on monotonic interval`
      (Req 7.1),
      `fix(telemetry): log cumulative gauge snapshots to MLflow` (Req 7.2),
      `fix(telemetry): write mlflow_export.json before uploading artifacts`
      (Req 7.3),
      `fix(telemetry): wire AIPERF_OTEL_MAX_BUFFERED_RECORDS to queue maxsize`
      (Req 7.4),
      `fix(plot): reject --dashboard --mlflow-upload before controller start`
      (Req 7.5), plus
      `style(telemetry): ...` (Req 8),
      `test(telemetry): ...` (Req 11),
      `docs(telemetry): ...` (Req 10).
    - A single squashed `feat(telemetry): ...` commit is also acceptable
      per Req 9.3, but the split form is preferred for review.
    - Do not force-push over any branch that exists on `ai-dynamo/aiperf`.
    - Requirements: 9.3, 9.6. Design: Rebase Strategy step 7.

  - [ ] 12.2 Ensure every commit has `Co-authored-by` + DCO sign-off
    - Every commit message must include the trailer
      `Co-authored-by: Emmanuel Bashorun <bashorun.emma@gmail.com>`.
    - Every commit must be DCO-signed (`git commit -s`, or add
      `Signed-off-by: <your name> <your email>` trailer).
    - Verify with `git -P log --pretty=full -n <count>` where `<count>`
      covers the takeover branch commits.
    - Requirements: 9.4. Design: Rebase Strategy step 8.

  - [ ] 12.3 Push the branch and open (or update) the PR
    - Default per §Assumptions: push
      `<current-user>/otel-mlflow-tracking` to `origin` and open a new
      PR that cross-links PR 656 and includes `Closes #656` in the
      description.
    - Fallback if push access on
      `briefgaming/aiperf:feat/otel-mlflow-tracking` is later confirmed
      (PR 656 has `maintainer_can_modify: true`): push to the fork
      branch instead and let PR #656 update in place. Do not open a new
      PR in that path.
    - PR description must enumerate the five defect fixes
      (Reqs 7.1-7.5), reference the two CodeRabbit review rounds, and
      link design.md and requirements.md for reviewer context.
    - Requirements: 9.5, 9.6. Design: Rebase Strategy step 9.

## Notes

- Tasks marked with `*` are optional (nice-to-have, not blocking merge).
  None are marked `*` in this plan because every task is either a design
  constraint, a requirement, or a defect fix.
- Each task cites specific requirement numbers and a design subsection for
  traceability. The requirement numbers match `requirements.md` §Acceptance
  Criteria; the design subsections match `design.md` §Components,
  §Architecture, §Data Models, §Correctness Properties, §Testing Strategy,
  §Documentation Plan, and §Rebase Strategy.
- Property-based tests are grouped in §8 for preserved behavior and
  placed next to their defect fix in §§3-5 for new behavior (P7 in §3,
  P4 in §4, P8 in §5). Each property test carries a header comment
  naming the feature and the property number.
- Integration tests (§9) are marked `@pytest.mark.component_integration`
  so they run under `uv run pytest -m component_integration`, not under
  the default unit suite.
- Verification gates (§11) mirror the `pre-commit` hook chain documented
  in AGENTS.md §Pre-Commit Hooks. Running `pre-commit run --all-files`
  once at the end is the aggregating check.

---

**Workflow status.** This `tasks.md` completes the feature-requirements-first
workflow for `otel-mlflow-telemetry-takeover`. The workflow produced
`requirements.md`, `design.md`, and this plan; it does not execute the
implementation. To begin executing tasks, open `tasks.md` in the Kiro
panel and click "Start task" next to a task item.
