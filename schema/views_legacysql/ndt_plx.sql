#legacySQL
-- All plx data, with DATE(_PARTITIONTIME) mapped to partition_date for proper
-- partition handling.
SELECT *
FROM (
  SELECT
    test_id,
    DATE(_PARTITIONTIME) AS partition_date,
    project, log_time, task_filename, parse_time, blacklist_flags,
    anomalies.*, connection_spec.*, web100_log_entry.*
  FROM
    [${PROJECT}:legacy.ndt] ),
  (
  SELECT
    test_id,
    DATE(_PARTITIONTIME) AS partition_date,
    project, log_time, task_filename, parse_time, blacklist_flags,
    anomalies.*, connection_spec.*, web100_log_entry.*
  FROM
    [${PROJECT}:legacy.ndt_pre2015] )
