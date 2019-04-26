#standardSQL
-- This is the sidestream root view that all other views in the sidestream dataset are derived from.
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `%s.base_tables.sidestream`
