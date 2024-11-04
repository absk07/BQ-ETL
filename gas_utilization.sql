CREATE OR REPLACE TABLE `deft-epoch-438812-b9.astronomer_poc.gas_utilization` AS
SELECT 
  b.block_number,
  b.block_timestamp,
  b.gas_used,
  b.gas_limit,
  ROUND(CAST(b.gas_used AS FLOAT64)/b.gas_limit * 100, 2) as gas_utilization_percentage,
  ROUND(b.base_fee_per_gas, 2) as base_fee
FROM `deft-epoch-438812-b9.astronomer_poc.stg_blocks` b
LEFT JOIN `deft-epoch-438812-b9.astronomer_poc.stg_transactions` t ON b.block_hash = t.block_hash
GROUP BY 1, 2, 3, 4, 6
ORDER BY b.block_number;