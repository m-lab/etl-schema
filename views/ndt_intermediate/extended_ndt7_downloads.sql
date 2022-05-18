--
-- NDT7 download data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab standard Unified Views.
--
-- Anything here that is not visible in the unified views is subject to
-- breaking changes.  Use with caution!
--
-- See the documentation on creating custom unified views.
--

WITH

ndt7downloads AS (
  SELECT *,
  raw.Download.ServerMeasurements[SAFE_ORDINAL(ARRAY_LENGTH(raw.Download.ServerMeasurements))] AS FinalSnapshot,
# (raw.Download.Error != "") AS IsErrored,  -- TODO ndt-server/issues/317
  False AS IsErrored,
  TIMESTAMP_DIFF(raw.Download.EndTime, raw.Download.StartTime, SECOND) AS test_duration
 FROM   `{{.ProjectID}}.ndt.ndt7`
  -- Limit to valid S2C results
  WHERE
    raw.Download IS NOT NULL
    AND raw.Download.UUID IS NOT NULL
    AND raw.Download.UUID NOT IN ( '', 'ERROR_DISCOVERING_UUID' )  -- TODO clear IsComplete instead
),

PreComputeNDT7 AS (
  SELECT

    -- All std columns top levels
    id, date, parser, server, client, a, raw,

    -- Computed above, due to sequential dependencies
    IsErrored, FinalSnapshot, test_duration,

    -- IsOAM
    ( raw.ClientIP IN
         -- TODO(m-lab/etl/issues/893): move to parser configuration.
        ( "35.193.254.117", -- script-exporter VMs in GCE, sandbox.
          "35.225.75.192", -- script-exporter VM in GCE, staging.
          "35.192.37.249", -- script-exporter VM in GCE, oti.
          "23.228.128.99", "2605:a601:f1ff:fffe::99", -- ks addresses.
          "45.56.98.222", "2600:3c03::f03c:91ff:fe33:819", -- eb addresses.
          "35.202.153.90", "35.188.150.110" -- Static IPs from GKE VMs for e2e tests.
        ) ) AS IsOAM, -- TODO Generalize

     -- _IsRFC1918  XXX deprecate?
     ( (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.ClientIP),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.ClientIP),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.ClientIP),
                16) = NET.IP_FROM_STRING("192.168.0.0")) ) AS _IsRFC1918, -- TODO does this matter?

    -- IsProduction TODO Check Server Metadata(?)
    REGEXP_CONTAINS(parser.ArchiveURL,
           'mlab[1-3]-[a-z][a-z][a-z][0-9][0-9]') AS IsProduction,

    -- Obsolete IsCongested and IsBloated, used by IsValid2021
    (FinalSnapshot.TCPInfo.TotalRetrans > 0) AS IsCongested,
    ((FinalSnapshot.TCPInfo.RTT > 2*FinalSnapshot.TCPInfo.MinRTT) AND
       (FinalSnapshot.TCPInfo.RTT > 1000)) AS IsBloated,

  FROM
    ndt7downloads
),

-- This must exactly match the Unified Download Schema
UnifiedDownloadSchema AS (
  SELECT
    id,
    date,
    STRUCT (
      a.UUID,
      a.TestTime,
      'Download' AS Direction,
      a.CongestionControl,
      a.MeanThroughputMbps,
      a.MinRTT,  -- mS
      a.LossRate
    ) AS a,

    STRUCT (
      'extended_ndt7_downloads' AS viewSource,
      CONCAT("ndt7",
            IF(raw.ClientIP LIKE "%:%", "-IPv6", "-IPv4"),
            CASE raw.ServerPort
                 WHEN 443 THEN "-WSS"
                 WHEN 80 THEN "-WS"
                 ELSE "-UNK" END ) AS NDTprotocol,
      raw.Download.ClientMetadata AS ClientMetadata,
      raw.Download.ServerMetadata AS ServerMetadata,
      [ parser ] AS Sources -- TODO add AnnotatonParser
    ) AS Metadata,

    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      FinalSnapshot IS NOT NULL AS IsComplete, -- Not Missing any key fields
      IsProduction,     -- Not mlab4, abc0t, or other pre production servers
      IsErrored,                  -- Server reported a problem
      IsOAM,               -- internal testing and monitoring
      _IsRFC1918,            -- Not a real client (deprecate?)
      False AS IsPlatformAnomaly, -- FUTURE, No switch discards, etc
      NULL AS WhatPlatformAnomaly,  -- FUTURE, what happened?
      (FinalSnapshot.TCPInfo.BytesAcked < 8192) AS IsShort, -- not enough data
      (test_duration < 9) AS IsAborted,   -- Did not run for enough time
      (test_duration > 60) AS IsHung,    -- Ran for too long
      IsCongested AS _IsCongested, -- XXX Deprecate?
      IsBloated AS _IsBloated -- XXX Deprecate?
    ) AS filter,
    -- NOTE: standard columns for views exclude the parseInfo struct because
    -- multiple tables are used to create a derived view. Users that want the
    -- underlying parseInfo values should refer to the corresponding tables
    -- using the shared UUID.
    STRUCT (
      raw.ClientIP AS IP,
      raw.ClientPort AS Port,
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
      raw.ServerIP AS IP,
      raw.ServerPort AS Port,
      server.Site, -- e.g. lga02
      server.Machine, -- e.g. mlab1
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
    PreComputeNDT7 AS _internal202205  -- Not stable and subject to breaking changes

  FROM PreComputeNDT7
)

SELECT * FROM UnifiedDownloadSchema
