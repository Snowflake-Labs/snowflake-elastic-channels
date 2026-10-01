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

-- Snowflake-managed Iceberg landing table + streaming pipe for the
-- elastic-channel Iceberg campaign. STRING (not VARCHAR(n)) is required:
-- Iceberg only allows max-length VARCHAR. CATALOG = 'SNOWFLAKE' uses
-- Snowflake-managed storage (no EXTERNAL_VOLUME / BASE_LOCATION).
--
-- ROW_TIMESTAMP is not a valid Iceberg table property, so
-- METADATA$ROW_LAST_COMMIT_TIME is unavailable. Ingest-to-visible latency
-- is measured from SALE_TIME to Iceberg file metadata (see analysis).

CREATE ICEBERG TABLE IF NOT EXISTS {database}.{schema}.RESORT_SALES_ICEBERG (
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
TARGET_FILE_SIZE = '128MB';

CREATE OR REPLACE PIPE {database}.{schema}.RESORT_SALES_ICEBERG_PIPE
AS COPY INTO {database}.{schema}.RESORT_SALES_ICEBERG
FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

GRANT SELECT, INSERT ON TABLE {database}.{schema}.RESORT_SALES_ICEBERG TO ROLE {ingest_role};
GRANT OPERATE ON PIPE {database}.{schema}.RESORT_SALES_ICEBERG_PIPE TO ROLE {ingest_role};
