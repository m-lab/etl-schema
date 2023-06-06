#standardSQL
-- Transform the v1 base_tables.ndt data into a static, v2-compatible schema.
-- Like tables produced by the v2 data pipeline, this static transformation will
-- create a table that is both date partitioned and requires a partition filter
-- for queries.
--
-- Always create within local project.
CREATE TABLE IF NOT EXISTS ndt.web100
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
	  ""             AS GitCommit,
	  0              AS ArchiveSize,
	  0              AS FileSize
   ) AS parser,
   STRUCT(
      connection_spec.ServerX.Site,
      connection_spec.ServerX.Machine,
      STRUCT(
        connection_spec.ServerX.Geo.ContinentCode,
        connection_spec.ServerX.Geo.CountryCode,
        connection_spec.ServerX.Geo.CountryCode3,
        connection_spec.ServerX.Geo.CountryName,
        CAST(NULL AS STRING) AS Region, -- mask out region.
        connection_spec.ServerX.Geo.Subdivision1ISOCode,
        connection_spec.ServerX.Geo.Subdivision1Name,
        connection_spec.ServerX.Geo.Subdivision2ISOCode,
        connection_spec.ServerX.Geo.Subdivision2Name,
        connection_spec.ServerX.Geo.MetroCode,
        connection_spec.ServerX.Geo.City,
        connection_spec.ServerX.Geo.AreaCode,
        connection_spec.ServerX.Geo.PostalCode,
        connection_spec.ServerX.Geo.Latitude,
        connection_spec.ServerX.Geo.Longitude,
        connection_spec.ServerX.Geo.AccuracyRadiusKm,
        connection_spec.ServerX.Geo.Missing
      ) AS Geo,
      connection_spec.ServerX.Network
   ) AS server,
   STRUCT(
      STRUCT(
        connection_spec.ClientX.Geo.ContinentCode,
        connection_spec.ClientX.Geo.CountryCode,
        connection_spec.ClientX.Geo.CountryCode3,
        connection_spec.ClientX.Geo.CountryName,
        CAST(NULL AS STRING) AS Region, -- mask out region.
        connection_spec.ClientX.Geo.Subdivision1ISOCode,
        connection_spec.ClientX.Geo.Subdivision1Name,
        connection_spec.ClientX.Geo.Subdivision2ISOCode,
        connection_spec.ClientX.Geo.Subdivision2Name,
        connection_spec.ClientX.Geo.MetroCode,
        connection_spec.ClientX.Geo.City,
        connection_spec.ClientX.Geo.AreaCode,
        connection_spec.ClientX.Geo.PostalCode,
        connection_spec.ClientX.Geo.Latitude,
        connection_spec.ClientX.Geo.Longitude,
        connection_spec.ClientX.Geo.AccuracyRadiusKm,
        connection_spec.ClientX.Geo.Missing
      ) AS Geo,
      connection_spec.ClientX.Network
    ) AS client,
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
