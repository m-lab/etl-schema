#standardSQL
-- This is the ndt5 root view that all other tcpinfo-platform views in the ndt dataset are derived from.
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `{{.ProjectID}}.base_tables.ndt5`
