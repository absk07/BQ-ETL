CREATE OR REPLACE TABLE `q-gcp-01778-ford-tm-23-12.astronomer_poc.receipts_transformed` AS
SELECT
  block_hash ,
  block_timestamp ,
  transaction_hash ,
  transaction_index ,
  from_address ,
  to_address ,
  contract_address ,
  cumulative_gas_used ,
  gas_used ,
  effective_gas_price ,
  root ,
  CASE 
    WHEN status = 1 THEN TRUE 
    WHEN status = 0 THEN FALSE
    ELSE NULL  -- Handle cases where the value isn't 1 or 0
  END AS status
FROM `q-gcp-01778-ford-tm-23-12.astronomer_poc.stg_receipts`