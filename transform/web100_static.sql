#standardSQL
-- Transform the v1 base_tables.ndt data into a static, v2-compatible schema.
-- Like tables produced by the v2 data pipeline, this static transformation will
-- create a table that is both date partitioned and requires a partition filter
-- for queries.
--
-- Always create within local project.
CREATE TABLE ndt.web100_static
PARTITION BY date
OPTIONS (
  require_partition_filter=true
)
AS
-- The schema should respect the standard top-level column name conventions:
--   * id
--   * date
--   * parser
--   * server
--   * client
--   * a
--   * raw
SELECT
   id,
   DATE(log_time) AS date,
   STRUCT(
     parser_version AS Version,
     parse_time     AS Time,
	  task_filename  AS ArchiveURL,
	  test_id        AS Filename,
	  0              AS Priority,
	  ""             AS GitCommit
   ) AS parser,
   connection_spec.ServerX AS server,
   connection_spec.ClientX AS client,
   IF(connection_spec.data_direction = 1,
     -- download.
     STRUCT(
        id AS UUID,
        log_time AS TestTime,
        "reno" AS CongestionControl,
        SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsAcked * 8.0,
           web100_log_entry.snap.SndLimTimeRwin +
           web100_log_entry.snap.SndLimTimeCwnd +
           web100_log_entry.snap.SndLimTimeSnd) AS MeanThroughputMbps,
        web100_log_entry.snap.MinRTT * 1.0 AS MinRTT,
        SAFE_DIVIDE(web100_log_entry.snap.SegsRetrans, web100_log_entry.snap.SegsOut) AS LossRate
     ),
     -- upload.
     STRUCT(
        id AS UUID,
        log_time AS TestTime,
        '' AS CongestionControl, -- https://github.com/m-lab/etl-schema/issues/95
        SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsReceived * 8.0, web100_log_entry.snap.Duration) AS MeanThroughputMbps,
        web100_log_entry.snap.MinRTT * 1.0 AS MinRTT,  -- Note: download side measurement (ms)
        NULL AS LossRate -- Receiver can not measure loss
   )) AS a,
   STRUCT(
       STRUCT(
        connection_spec.client_af,
        connection_spec.client_application,
        connection_spec.client_browser,
        connection_spec.client_hostname,
        connection_spec.client_ip,
        connection_spec.client_kernel_version,
        connection_spec.client_os,
        connection_spec.client_version,
        connection_spec.data_direction,
        connection_spec.server_af,
        connection_spec.server_hostname,
        connection_spec.server_ip,
        connection_spec.server_kernel_version,
        connection_spec.tls,
        connection_spec.websockets
       ) AS connection,
       web100_log_entry AS web100
   ) AS raw,
FROM `mlab-oti.base_tables.ndt`
