#standardSQL
-- This is the utilization root view of the switch dataset.
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `%s.base_tables.switch`
