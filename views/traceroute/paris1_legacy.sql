-- This is the aggregated paris1 view.
-- Currenlty, it only contains data from ndt.
-- In the future, it can be combined with other experiments.
--
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `{{.ProjectID}}.base_tables.traceroute
