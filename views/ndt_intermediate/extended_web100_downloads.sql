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
    *,
    raw.web100.snap.Duration AS connection_duration, -- SYN to FIN total time
    (raw.web100.snap.SndLimTimeRwin +
         raw.web100.snap.SndLimTimeCwnd +
         raw.web100.snap.SndLimTimeSnd) AS measurement_duration, -- Time transfering data
    -- TODO: restore when blacklist flags (or alternate name) is restored.
    -- (blacklist_flags IS NOT NULL and blacklist_flags != 0
    --    OR anomalies.blacklist_flags IS NOT NULL ) AS IsErrored,
    (raw.web100.connection_spec.remote_ip IN
          ("45.56.98.222", "35.192.37.249", "35.225.75.192", "23.228.128.99",
          "2600:3c03::f03c:91ff:fe33:819", "2605:a601:f1ff:fffe::99")
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.web100.connection_spec.local_ip),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.web100.connection_spec.local_ip),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.web100.connection_spec.local_ip),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
        OR REGEXP_EXTRACT(parser.ArchiveURL, '(mlab[1-4])-[a-z][a-z][a-z][0-9][0-9t]') = 'mlab4'
     ) AS IsOAM,  -- Data is not from valid clients
     raw.web100.snap.OctetsRetrans > 0 AS IsCongested,
     (  raw.web100.snap.SmoothedRTT > 2*raw.web100.snap.MinRTT AND
        raw.web100.snap.SmoothedRTT > 1000 ) AS IsBloated,
    STRUCT (
      parser.Version,
      parser.Time,
      parser.ArchiveURL,
      parser.Filename
    ) AS Web100parser,
  FROM `{{.ProjectID}}.ndt.web100_static`
  WHERE
    raw.web100.snap.Duration IS NOT NULL
    AND raw.web100.snap.State IS NOT NULL
    AND raw.web100.connection_spec.local_ip IS NOT NULL
    AND raw.web100.connection_spec.remote_ip IS NOT NULL
    AND raw.web100.snap.SndLimTimeRwin IS NOT NULL
    AND raw.web100.snap.SndLimTimeCwnd IS NOT NULL
    AND raw.web100.snap.SndLimTimeSnd IS NOT NULL
),

Web100DownloadModels AS (
  SELECT
    id,
    date,
    -- Struct a models various TCP behaviors
    STRUCT(
      id as UUID,
      a.TestTime,
      "reno" AS CongestionControl,
      raw.web100.snap.HCThruOctetsAcked * 8.0 / measurement_duration AS MeanThroughputMbps,
      raw.web100.snap.MinRTT * 1.0 AS MinRTT,
      SAFE_DIVIDE(raw.web100.snap.SegsRetrans, raw.web100.snap.SegsOut) AS LossRate
    ) AS a,
    STRUCT (
     "web100" AS _Instruments -- THIS WILL CHANGE
    ) AS node,
    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      ( -- Download only, >8kB transfered, 9-60 seconds, network bottlenck
        NOT IsOAM -- AND NOT IsErrored
        AND raw.connection.data_direction IS NOT NULL
        AND raw.connection.data_direction = 1
        AND raw.web100.snap.HCThruOctetsAcked IS NOT NULL
        AND raw.web100.snap.HCThruOctetsAcked >= 8192
        AND measurement_duration BETWEEN 9000000 AND 60000000
        AND (IsCongested OR IsBloated)
      ) AS IsValidBest,
      ( -- Download only, >kB transfered, 9-60 seconds, network bottlenck
        NOT IsOAM -- AND NOT IsErrored
        AND raw.connection.data_direction IS NOT NULL
        AND raw.connection.data_direction = 1
        AND raw.web100.snap.HCThruOctetsAcked IS NOT NULL
        AND raw.web100.snap.HCThruOctetsAcked >= 8192
        AND measurement_duration BETWEEN 9000000 AND 60000000
        AND (IsCongested) -- Does not include buffer bloat
      ) AS IsValid2019
    ) AS filter,
    STRUCT (
      -- TODO(soltesz): eliminate ip / port from server/client records.
      raw.web100.connection_spec.remote_ip AS IP,
      raw.web100.connection_spec.remote_port AS Port,
      client.Geo,
      client.Network
    ) AS client,
    STRUCT (
      -- TODO(soltesz): eliminate ip / port from server/client records.
      raw.web100.connection_spec.local_ip AS IP,
      raw.web100.connection_spec.local_port AS Port,
      server.Site,
      server.Machine,
      server.Geo,
      server.Network
    ) AS server,
    PreCleanWeb100 AS _internal202010  -- Not stable and subject to breaking changes
  FROM PreCleanWeb100
  WHERE
    measurement_duration > 0 AND connection_duration > 0
)

SELECT * FROM Web100DownloadModels
