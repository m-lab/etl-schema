#standardSQL
-- This was the root view under the old parser.
-- It is now obsolete, and will be deprecated once the new parser is fully in production.
--
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `{{.ProjectID}}.base_tables.ndt`
