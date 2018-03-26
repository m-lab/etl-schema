#standardSQL
-- All plx data, with _PARTITIONDATE mapped to partition_date for proper
-- partition handling.
SELECT
  test_id,
  _PARTITIONDATE AS partition_date,
  project, log_time, task_filename, parse_time, blacklist_flags,
  anomalies,
  connection_spec,
  web100_log_entry
FROM `mlab-sandbox.legacy.ndt`
UNION ALL
SELECT
  test_id,
  _PARTITIONDATE AS partition_date,
  project, log_time, task_filename, parse_time, blacklist_flags,
  anomalies,
  connection_spec,
  web100_log_entry
FROM `mlab-sandbox.legacy.ndt_pre2015`