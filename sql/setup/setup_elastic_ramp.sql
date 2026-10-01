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

-- Elastic channel ramp test: resort point-of-sale landing table and streaming pipe.

CREATE TABLE IF NOT EXISTS RESORT_SALES (
    TXID VARCHAR(255) NOT NULL,
    RESORT VARCHAR(255) NOT NULL,
    CATEGORY VARCHAR(32) NOT NULL,
    ITEM VARCHAR(255) NOT NULL,
    CUSTOMER_TYPE VARCHAR(32) NOT NULL,
    PAYMENT_METHOD VARCHAR(32) NOT NULL,
    QUANTITY INTEGER NOT NULL,
    UNIT_PRICE NUMBER(10, 2) NOT NULL,
    AMOUNT NUMBER(10, 2) NOT NULL,
    SALE_TIME TIMESTAMP_NTZ NOT NULL,
    RUN_ID VARCHAR(128),
    WORKER_ID INTEGER,
    INGEST_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
) ROW_TIMESTAMP = TRUE;

ALTER TABLE {database}.{schema}.RESORT_SALES SET ROW_TIMESTAMP = TRUE;

-- Custom pipe (not Snowpipe Streaming's implicit per-table default pipe, which lands
-- everything in a single VARIANT column). MATCH_BY_COLUMN_NAME = CASE_SENSITIVE maps
-- payload keys straight to RESORT_SALES's columns by exact-case name, skipping the
-- $1:FIELD::TYPE extraction step entirely (cheaper + faster per Snowflake's Snowpipe
-- Streaming high-performance best practices: avoids materializing the full row into a
-- VARIANT before extracting columns, and only bills for ingested column bytes).
-- Requires ResortSalesGenerator.next_batch() payload keys to stay uppercase and identical
-- to these column names (TXID, RESORT, CATEGORY, ITEM, CUSTOMER_TYPE, PAYMENT_METHOD,
-- QUANTITY, UNIT_PRICE, AMOUNT, SALE_TIME, RUN_ID, WORKER_ID) — CASE_SENSITIVE match means
-- "txid" or "Txid" would NOT match "TXID".
-- NOTE: INGEST_TS has no corresponding payload key, so MATCH_BY_COLUMN_NAME loads it as
-- NULL (copy options never apply column DEFAULTs, only plain INSERT does). Latency
-- analysis uses METADATA$ROW_LAST_COMMIT_TIME instead of INGEST_TS (see
-- k8s_monitor.py), so this has no functional impact.
CREATE OR REPLACE PIPE RESORT_SALES_PIPE
AS COPY INTO RESORT_SALES
FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

GRANT USAGE ON DATABASE {database} TO ROLE {ingest_role};
GRANT USAGE ON SCHEMA {database}.{schema} TO ROLE {ingest_role};
GRANT SELECT, INSERT ON TABLE {database}.{schema}.RESORT_SALES TO ROLE {ingest_role};
GRANT OPERATE ON PIPE {database}.{schema}.RESORT_SALES_PIPE TO ROLE {ingest_role};

-- Tidy up the earlier lift-ride-schema objects when re-running setup against
-- an account that still has them (harmless no-op otherwise).
DROP PIPE IF EXISTS {database}.{schema}.LIFT_RIDE_PIPE;
DROP TABLE IF EXISTS {database}.{schema}.LIFT_RIDE;
