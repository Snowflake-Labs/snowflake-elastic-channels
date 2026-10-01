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

-- Clustered Iceberg landing table with a 16MB compaction target (vs
-- RESORT_SALES_ICEBERG_CLUSTERED at 128MB). Same payload and CLUSTER BY
-- (DATE_TRUNC('DAY', SALE_TIME), RESORT). Separate table + pipe so the
-- 128MB twin is left untouched.
--
-- Snowflake-managed Iceberg: STRING columns, CATALOG = 'SNOWFLAKE'.

CREATE ICEBERG TABLE IF NOT EXISTS {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED_16MB (
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
TARGET_FILE_SIZE = '16MB'
CLUSTER BY (DATE_TRUNC('DAY', SALE_TIME), RESORT);

CREATE OR REPLACE PIPE {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED_16MB_PIPE
AS COPY INTO {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED_16MB
FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

GRANT SELECT, INSERT ON TABLE {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED_16MB TO ROLE {ingest_role};
GRANT OPERATE ON PIPE {database}.{schema}.RESORT_SALES_ICEBERG_CLUSTERED_16MB_PIPE TO ROLE {ingest_role};
