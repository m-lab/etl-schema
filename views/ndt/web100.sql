#standardSQL
-- This is the ndt root view that all other views in the ndt dataset are derived from.
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `{{.ProjectID}}.base_tables.ndt`
