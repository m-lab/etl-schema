--
-- Legacy NDT/web100 upload data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab standard Unified Views.
--
-- Anything here that is not visible in the unified views is subject to
-- breaking changes.  Use with caution!
--
-- See the documentation on creating custom unified views.
--

WITH PreCleanWeb100 AS (
  SELECT
    -- NOTE: we name the partition_date to test_date to prevent exposing
    -- implementation details that are expected to change.
    partition_date AS date,
    *,
    web100_log_entry.snap.Duration AS connection_duration, -- SYN to FIN total time
    IF(web100_log_entry.snap.Duration > 12000000,   /* 12 sec */
       web100_log_entry.snap.Duration - 2000000,
       web100_log_entry.snap.Duration) AS measurement_duration, -- Time transfering data
    (blacklist_flags IS NOT NULL and blacklist_flags != 0
        OR anomalies.blacklist_flags IS NOT NULL ) AS IsErrored,
    (web100_log_entry.connection_spec.remote_ip IN
          ("45.56.98.222", "35.192.37.249", "35.225.75.192", "23.228.128.99",
          "2600:3c03::f03c:91ff:fe33:819", "2605:a601:f1ff:fffe::99")
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(web100_log_entry.connection_spec.local_ip),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(web100_log_entry.connection_spec.local_ip),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(web100_log_entry.connection_spec.local_ip),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
        OR REGEXP_EXTRACT(task_filename, '(mlab[1-4])-[a-z][a-z][a-z][0-9][0-9t]') = 'mlab4'
     ) AS IsOAM,  -- Data is not from valid clients
     ( -- Eliminate some clearly bogus data
       web100_log_entry.snap.HCThruOctetsReceived > 1E14 -- approximately 10Gb/s for 24 hours
     ) AS IsCorrupted,
    STRUCT (
      parser_version AS Version,
      parse_time AS Time,
      task_filename AS ArchiveURL,
      "web100" AS Filename
    ) AS Web100parser,
  FROM `{{.ProjectID}}.ndt_raw.web100_legacy` -- TODO move to intermediate_ndt
  WHERE
    web100_log_entry.snap.Duration IS NOT NULL
    AND web100_log_entry.snap.State IS NOT NULL
    AND web100_log_entry.connection_spec.local_ip IS NOT NULL
    AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
    AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
    AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
    AND web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
),

Web100UploadModels AS (
  SELECT
    id,
    date,
    -- Struct a models various TCP behaviors
    STRUCT(
      id as UUID,
      log_time AS TestTime,
      '' AS CongestionControl, -- https://github.com/m-lab/etl-schema/issues/95
      web100_log_entry.snap.HCThruOctetsReceived * 8.0 / connection_duration AS MeanThroughputMbps,
      web100_log_entry.snap.MinRTT * 1.0 AS MinRTT,  -- Note: download side measurement (ms)
      Null AS LossRate -- Receiver can not measure loss
    ) AS a,
    STRUCT (
     "web100" AS _Instruments -- THIS WILL CHANGE
    ) AS node,
    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      ( -- Upload only, >8kB transfered, 9-60 seconds
        NOT IsOAM AND NOT IsErrored AND NOT IsCorrupted
        AND connection_spec.data_direction IS NOT NULL
        AND connection_spec.data_direction = 0
        AND web100_log_entry.snap.HCThruOctetsReceived IS NOT NULL
        AND web100_log_entry.snap.HCThruOctetsReceived >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
        ) AS IsValidBest,
      ( -- Upload only, >8kB transfered, 9-60 seconds
        NOT IsOAM AND NOT IsErrored
        AND connection_spec.data_direction IS NOT NULL
        AND connection_spec.data_direction = 0
        AND web100_log_entry.snap.HCThruOctetsReceived IS NOT NULL
        AND web100_log_entry.snap.HCThruOctetsReceived >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
      ) AS IsValid2019
    ) AS filter,
    STRUCT (
      web100_log_entry.connection_spec.remote_ip AS IP,
      web100_log_entry.connection_spec.remote_port AS Port,
      -- TODO(https://github.com/m-lab/etl/issues/1069): eliminate region mask once parser does this.
      STRUCT(
        connection_spec.ClientX.Geo.ContinentCode,
        connection_spec.ClientX.Geo.CountryCode,
        connection_spec.ClientX.Geo.CountryCode3,
        connection_spec.ClientX.Geo.CountryName,
        "", -- mask out region.
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
    STRUCT (
      web100_log_entry.connection_spec.local_ip AS IP,
      web100_log_entry.connection_spec.local_port AS Port,
      connection_spec.ServerX.Site,
      connection_spec.ServerX.Machine,
      -- TODO(https://github.com/m-lab/etl/issues/1069): eliminate region mask once parser does this.
      STRUCT(
        connection_spec.ServerX.Geo.ContinentCode,
        connection_spec.ServerX.Geo.CountryCode,
        connection_spec.ServerX.Geo.CountryCode3,
        connection_spec.ServerX.Geo.CountryName,
        "", -- mask out region.
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
    PreCleanWeb100 AS _internal202010  -- Not stable and subject to breaking changes
  FROM PreCleanWeb100
  WHERE
    measurement_duration > 0 AND connection_duration > 0
)

SELECT * FROM Web100UploadModels
