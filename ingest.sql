CREATE OR REPLACE TABLE `deft-epoch-438812-b9.astronomer_poc.stg_blocks` AS
SELECT * FROM `bigquery-public-data.goog_blockchain_polygon_mainnet_us.blocks`
LIMIT 1000;

CREATE OR REPLACE TABLE `deft-epoch-438812-b9.astronomer_poc.stg_transactions` AS
SELECT * FROM `bigquery-public-data.goog_blockchain_polygon_mainnet_us.transactions`
LIMIT 1000;

CREATE OR REPLACE TABLE `deft-epoch-438812-b9.astronomer_poc.stg_receipts` AS
SELECT * FROM `bigquery-public-data.goog_blockchain_polygon_mainnet_us.receipts`
LIMIT 1000;

CREATE OR REPLACE TABLE `deft-epoch-438812-b9.astronomer_poc.stg_logs` AS
SELECT * FROM `bigquery-public-data.goog_blockchain_polygon_mainnet_us.logs`
LIMIT 1000;