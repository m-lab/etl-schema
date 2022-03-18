--
-- Legacy NDT/web100 downloads data in standard columns plus additional annotations.
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
    (web100_log_entry.snap.SndLimTimeRwin +
         web100_log_entry.snap.SndLimTimeCwnd +
         web100_log_entry.snap.SndLimTimeSnd) AS measurement_duration, -- Time transfering data
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
     web100_log_entry.snap.OctetsRetrans > 0 AS IsCongested,
     (  web100_log_entry.snap.SmoothedRTT > 2*web100_log_entry.snap.MinRTT AND
        web100_log_entry.snap.SmoothedRTT > 1000 ) AS IsBloated,
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

Web100DownloadModels AS (
  SELECT
    id,
    date,
    -- Struct a models various TCP behaviors
    STRUCT(
      id as UUID,
      log_time AS TestTime,
      "reno" AS CongestionControl,
      web100_log_entry.snap.HCThruOctetsAcked * 8.0 / measurement_duration AS MeanThroughputMbps,
      web100_log_entry.snap.MinRTT * 1.0 AS MinRTT,
      SAFE_DIVIDE(web100_log_entry.snap.SegsRetrans, web100_log_entry.snap.SegsOut) AS LossRate
    ) AS a,
    STRUCT (
     "web100" AS _Instruments -- THIS WILL CHANGE
    ) AS node,
    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      ( -- Download only, >8kB transfered, 9-60 seconds, network bottlenck
        NOT IsOAM AND NOT IsErrored
        AND connection_spec.data_direction IS NOT NULL
        AND connection_spec.data_direction = 1
        AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
        AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
        AND measurement_duration BETWEEN 9000000 AND 60000000
        AND (IsCongested OR IsBloated)
      ) AS IsValidBest,
      ( -- Download only, >kB transfered, 9-60 seconds, network bottlenck
        NOT IsOAM AND NOT IsErrored
        AND connection_spec.data_direction IS NOT NULL
        AND connection_spec.data_direction = 1
        AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
        AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
        AND measurement_duration BETWEEN 9000000 AND 60000000
        AND (IsCongested) -- Does not include buffer bloat
      ) AS IsValid2019
    ) AS filter,
    STRUCT (
      web100_log_entry.connection_spec.remote_ip AS IP,
      web100_log_entry.connection_spec.remote_port AS Port,
      connection_spec.ClientX.Geo,
      connection_spec.ClientX.Network,
    ) AS client,
    STRUCT (
      web100_log_entry.connection_spec.local_ip AS IP,
      web100_log_entry.connection_spec.local_port AS Port,
      connection_spec.ServerX.Site,
      connection_spec.ServerX.Machine,
      connection_spec.ServerX.Geo,
      connection_spec.ServerX.Network,
    ) AS server,
    PreCleanWeb100 AS _internal202010  -- Not stable and subject to breaking changes
  FROM PreCleanWeb100
  WHERE
    measurement_duration > 0 AND connection_duration > 0
)

SELECT * FROM Web100DownloadModels
