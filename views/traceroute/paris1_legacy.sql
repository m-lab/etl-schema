-- This is the aggregated paris1 view from the web100 platform.
-- This includes measurements to ndt and all other measurement services.
--
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `{{.ProjectID}}.base_tables.traceroute`
