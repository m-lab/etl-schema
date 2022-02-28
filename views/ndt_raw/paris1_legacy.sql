#standardSQL
-- This was the root paris-traceroute view under the old parser.
-- It will be deprecated once the new parser is in production.
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `{{.ProjectID}}.base_tables.traceroute`
