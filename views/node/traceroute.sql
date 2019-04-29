#standardSQL
-- This is the traceroute root view for historical traceroute data.
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `%s.base_tables.traceroute`
