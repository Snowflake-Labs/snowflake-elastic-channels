-- Copyright 2026 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Table the cluster-resident monitor (src/elastic_channels/k8s_monitor.py) writes
-- aggregated live performance data to, one row per aggregation window per run.
-- Populated *during* the run directly from the cluster, so it can be queried live
-- (e.g. from streamlit/Monitor_App.py) while workers are still ingesting.
--
-- HYBRID TABLE: the monitor does a single-row INSERT every flush cycle
-- (ELASTIC_MONITOR_FLUSH_SECONDS, default 10s) per active run, and dashboards/scripts
-- read it live with point/range lookups (`WHERE RUN_ID = ? ORDER BY WINDOW_START` —
-- see scripts/run_worker_scale_probe.py). That one-row-at-a-time write + point-lookup
-- pattern is the OLTP-style access hybrid tables are built for (row-based storage engine,
-- index-organized on the primary key) rather than a standard table's micro-partition
-- model, which is tuned for bulk/batched writes, not a frequent trickle of tiny inserts.
-- PRIMARY KEY (RUN_ID, WINDOW_START) is required for hybrid tables and doubles as the
-- index that makes the `WHERE RUN_ID = ? ORDER BY WINDOW_START` read pattern a direct,
-- sorted range scan instead of a full-table scan. WINDOW_START is unique per run because
-- the monitor advances one shared, monotonically increasing window per flush cycle
-- (see K8sMonitor.run()).
--
-- Migrating an existing standard-table deployment to hybrid cannot be done with
-- ALTER TABLE (table type can't be changed in place) or CREATE TABLE ... IF NOT EXISTS
-- (a same-named object of the wrong type already existing makes IF NOT EXISTS a no-op,
-- silently keeping the old standard table) — scripts/setup_elastic_ramp.py handles that
-- one-time rename + data-copy + drop in Python before running this file; this file only
-- needs to describe the desired (hybrid) end state.

CREATE HYBRID TABLE IF NOT EXISTS ELASTIC_K8S_METRICS (
    RUN_ID VARCHAR(128) NOT NULL,
    WINDOW_START TIMESTAMP_NTZ NOT NULL,
    WINDOW_END TIMESTAMP_NTZ NOT NULL,
    WORKERS_REPORTING INTEGER,
    WORKERS_EXPECTED INTEGER,
    ROWS_PER_SEC FLOAT,
    UNCOMPRESSED_BYTES_PER_SEC FLOAT,
    COMPRESSED_BYTES_PER_SEC FLOAT,
    COMPRESSION_RATIO FLOAT,
    AVG_BATCH_ACK_MS FLOAT,
    MAX_BATCH_ACK_MS FLOAT,
    BATCHES_ACKED INTEGER,
    ERRORS_TOTAL INTEGER,
    -- Fleet-wide sum, at flush time, of each worker's cumulative
    -- `sf_api_retryable_error_count` SDK metric (429/503 responses the SDK retried
    -- internally) — a *throttling* signal distinct from ERRORS_TOTAL (hard,
    -- unretryable append/commit failures). Non-decreasing per worker within a run;
    -- summed across currently-reporting workers, not delta'd, same as ERRORS_TOTAL.
    RETRYABLE_ERRORS_TOTAL INTEGER,
    -- Populated only on windows where the monitor ran the (comparatively expensive)
    -- SALE_TIME -> METADATA$ROW_LAST_COMMIT_TIME query; see
    -- ELASTIC_MONITOR_COMMIT_LATENCY_SECONDS in k8s_monitor.py. NULL on the
    -- windows in between, so `WHERE AVG_COMMIT_LATENCY_MS IS NOT NULL` isolates them.
    AVG_COMMIT_LATENCY_MS FLOAT,
    MAX_COMMIT_LATENCY_MS FLOAT,
    COMMIT_LATENCY_ROWS_SAMPLED INTEGER,
    INSERTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT ELASTIC_K8S_METRICS_PK PRIMARY KEY (RUN_ID, WINDOW_START)
);

-- Idempotent migration for tables created before RETRYABLE_ERRORS_TOTAL existed
-- (CREATE HYBRID TABLE IF NOT EXISTS above is a no-op against an already-live table).
ALTER TABLE ELASTIC_K8S_METRICS ADD COLUMN IF NOT EXISTS RETRYABLE_ERRORS_TOTAL INTEGER;

GRANT SELECT, INSERT ON TABLE {database}.{schema}.ELASTIC_K8S_METRICS TO ROLE {ingest_role};
