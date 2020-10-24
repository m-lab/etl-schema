#standardSQL
-- This was the root view under the old parser.
-- It is now obsolete, and will be deprecated once the new parser is fully in production.
--
-- TODO(https://github.com/m-lab/etl-schema/issues/49)
-- The tcp-info data ought to be partitioned on a per-experiment basis. Right now this links to a
-- too-large table of tcpinfo data that contains a superset of what it should.
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `{{.ProjectID}}.base_tables.tcpinfo`
