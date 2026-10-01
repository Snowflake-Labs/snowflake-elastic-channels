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

-- Clustered comparison tables: same RESORT_SALES payload shape as
-- RESORT_SALES_FDN / RESORT_SALES_ICEBERG, but with an explicit clustering
-- key (sale day, resort) to test whether Automatic Clustering drives the
-- file-merge/compaction activity that was absent without one (see
-- docs/analysis/iceberg-ingest-to-visible-latency.md: ENABLE_DATA_COMPACTION
-- was on for the unclustered tables, but ICEBERG_STORAGE_OPTIMIZATION_HISTORY
-- showed zero bytes scanned / rows written across every logged window).
-- Snowflake docs: "If you have Automatic Clustering enabled, clustering
-- performs data compaction on the table. This is true, regardless of
-- whether data compaction is enabled or disabled on the table."
--
-- Different table + pipe names on purpose: this leaves RESORT_SALES,
-- RESORT_SALES_FDN, and RESORT_SALES_ICEBERG (and their data) untouched.

CREATE TABLE IF NOT EXISTS {database}.{schema}.RESORT_SALES_FDN_CLUSTERED (
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
)
CLUSTER BY (DATE_TRUNC('DAY', SALE_TIME), RESORT);

CREATE OR REPLACE PIPE {database}.{schema}.RESORT_SALES_FDN_CLUSTERED_PIPE
AS COPY INTO {database}.{schema}.RESORT_SALES_FDN_CLUSTERED
FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- STRING (not VARCHAR(n)) for the Iceberg twin: Iceberg only allows
-- max-length VARCHAR. CATALOG = 'SNOWFLAKE' + EXTERNAL_VOLUME =
-- 'SNOWFLAKE_MANAGED' matches RESORT_SALES_ICEBERG's setup (Snowflake-managed
-- storage). TARGET_FILE_SIZE = '128MB' matches the RESORT_SALES_ICEBERG
-- rerun so the clustered comparison isn't confounded by a different target.
CREATE ICEBERG TABLE IF NOT EXISTS {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED (
    TXID STRING,
    RESORT STRING,
    CATEGORY STRING,
    ITEM STRING,
    CUSTOMER_TYPE STRING,
    PAYMENT_METHOD STRING,
    QUANTITY INT,
    UNIT_PRICE DECIMAL(10, 2),
    AMOUNT DECIMAL(10, 2),
    SALE_TIME TIMESTAMP_NTZ,
    RUN_ID STRING,
    WORKER_ID INT
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
TARGET_FILE_SIZE = '128MB'
CLUSTER BY (DATE_TRUNC('DAY', SALE_TIME), RESORT);

CREATE OR REPLACE PIPE {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED_PIPE
AS COPY INTO {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED
FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

GRANT SELECT, INSERT ON TABLE {database}.{schema}.RESORT_SALES_FDN_CLUSTERED TO ROLE {ingest_role};
GRANT OPERATE ON PIPE {database}.{schema}.RESORT_SALES_FDN_CLUSTERED_PIPE TO ROLE {ingest_role};
GRANT SELECT, INSERT ON TABLE {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED TO ROLE {ingest_role};
GRANT OPERATE ON PIPE {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED_PIPE TO ROLE {ingest_role};
