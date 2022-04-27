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
    IF(raw.web100.snap.Duration > 12000000,   /* 12 sec */
       raw.web100.snap.Duration - 2000000,
       raw.web100.snap.Duration) AS measurement_duration, -- Time transfering data
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
     ( -- Eliminate some clearly bogus data
       raw.web100.snap.HCThruOctetsReceived > 1E14 -- approximately 10Gb/s for 24 hours
     ) AS IsCorrupted,
    STRUCT (
      parser.Version AS Version,
      parser.Time AS Time,
      parser.ArchiveURL AS ArchiveURL,
      "web100" AS Filename
    ) AS Web100parser,
  FROM `{{.ProjectID}}.ndt.web100_static` -- TODO move to intermediate_ndt
  WHERE
    raw.web100.snap.Duration IS NOT NULL
    AND raw.web100.snap.State IS NOT NULL
    AND raw.web100.connection_spec.local_ip IS NOT NULL
    AND raw.web100.connection_spec.remote_ip IS NOT NULL
    AND raw.web100.snap.SndLimTimeRwin IS NOT NULL
    AND raw.web100.snap.SndLimTimeCwnd IS NOT NULL
    AND raw.web100.snap.SndLimTimeSnd IS NOT NULL
),

Web100UploadModels AS (
  SELECT
    id,
    date,
    -- Struct a models various TCP behaviors
    STRUCT(
      id as UUID,
      a.TestTime AS TestTime,
      '' AS CongestionControl, -- https://github.com/m-lab/etl-schema/issues/95
      raw.web100.snap.HCThruOctetsReceived * 8.0 / connection_duration AS MeanThroughputMbps,
      raw.web100.snap.MinRTT * 1.0 AS MinRTT,  -- Note: download side measurement (ms)
      Null AS LossRate -- Receiver can not measure loss
    ) AS a,
    STRUCT (
     "web100" AS _Instruments -- THIS WILL CHANGE
    ) AS node,
    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      ( -- Upload only, >8kB transfered, 9-60 seconds
        NOT IsOAM -- AND NOT IsErrored
        AND NOT IsCorrupted
        AND raw.connection.data_direction IS NOT NULL
        AND raw.connection.data_direction = 0
        AND raw.web100.snap.HCThruOctetsReceived IS NOT NULL
        AND raw.web100.snap.HCThruOctetsReceived >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
        ) AS IsValidBest,
      ( -- Upload only, >8kB transfered, 9-60 seconds
        NOT IsOAM -- AND NOT IsErrored
        AND raw.connection.data_direction IS NOT NULL
        AND raw.connection.data_direction = 0
        AND raw.web100.snap.HCThruOctetsReceived IS NOT NULL
        AND raw.web100.snap.HCThruOctetsReceived >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
      ) AS IsValid2019
    ) AS filter,
    STRUCT (
      raw.web100.connection_spec.remote_ip AS IP,
      raw.web100.connection_spec.remote_port AS Port,
      -- TODO(https://github.com/m-lab/etl/issues/1069): eliminate region mask once parser does this.
      STRUCT(
        client.Geo.ContinentCode,
        client.Geo.CountryCode,
        client.Geo.CountryCode3,
        client.Geo.CountryName,
        CAST(NULL as STRING) as Region, -- mask out region.
        client.Geo.Subdivision1ISOCode,
        client.Geo.Subdivision1Name,
        client.Geo.Subdivision2ISOCode,
        client.Geo.Subdivision2Name,
        client.Geo.MetroCode,
        client.Geo.City,
        client.Geo.AreaCode,
        client.Geo.PostalCode,
        client.Geo.Latitude,
        client.Geo.Longitude,
        client.Geo.AccuracyRadiusKm,
        client.Geo.Missing
      ) AS Geo,
      client.Network
    ) AS client,
    STRUCT (
      raw.web100.connection_spec.local_ip AS IP,
      raw.web100.connection_spec.local_port AS Port,
      server.Site,
      server.Machine,
      -- TODO(https://github.com/m-lab/etl/issues/1069): eliminate region mask once parser does this.
      STRUCT(
        server.Geo.ContinentCode,
        server.Geo.CountryCode,
        server.Geo.CountryCode3,
        server.Geo.CountryName,
        CAST(NULL as STRING) as Region, -- mask out region.
        server.Geo.Subdivision1ISOCode,
        server.Geo.Subdivision1Name,
        server.Geo.Subdivision2ISOCode,
        server.Geo.Subdivision2Name,
        server.Geo.MetroCode,
        server.Geo.City,
        server.Geo.AreaCode,
        server.Geo.PostalCode,
        server.Geo.Latitude,
        server.Geo.Longitude,
        server.Geo.AccuracyRadiusKm,
        server.Geo.Missing
      ) AS Geo,
      server.Network
    ) AS server,
    PreCleanWeb100 AS _internal202010  -- Not stable and subject to breaking changes
  FROM PreCleanWeb100
  WHERE
    measurement_duration > 0 AND connection_duration > 0
)

SELECT * FROM Web100UploadModels
