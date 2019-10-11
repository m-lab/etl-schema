#standardSQL
-- This is the root view that all other views in the ndt dataset are derived from.
--
-- TODO(https://github.com/m-lab/etl-schema/issues/49)
-- The tcp-info data ought to be partitioned on a per-experiment basis. Right now this links to a
-- too-large table of tcpinfo data that contains a superset of what it should.
SELECT CAST(_PARTITIONTIME AS DATE) AS partition_date, *
FROM `{{.ProjectID}}.base_tables.tcpinfo`
