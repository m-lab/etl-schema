--
-- NDT5 download data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab standard Unified Views.
--
-- Anything here that is not visible in the unified views is subject to
-- breaking changes.  Use with caution!
--
-- See the documentation on creating custom unified views.
--

WITH

ndt5downloads AS (
  SELECT id, date, a, parser AS NDTparser, raw.S2C, raw, client, server,
    (raw.S2C.Error IS NOT NULL AND raw.S2C.Error != "") AS IsErrored,
    TIMESTAMP_DIFF(raw.S2C.EndTime, raw.S2C.StartTime, SECOND) AS test_duration
  FROM   `{{.ProjectID}}.ndt.ndt5`
  -- Limit to valid S2C results
  WHERE raw.S2C IS NOT NULL
    AND raw.S2C.UUID IS NOT NULL
    AND raw.S2C.UUID NOT IN ( '', 'ERROR_DISCOVERING_UUID' )  -- TODO clear IsComplete instead
),

tcpinfo AS (
  SELECT * EXCEPT(a, parser, raw),
    a.FinalSnapshot,
    parser AS TCPparser,
  FROM `{{.ProjectID}}.ndt_raw.tcpinfo`
),

PreComputeNDT5 AS (
  SELECT
    downloads.*,
    FinalSnapshot, raw.Control,
    -- TODO (add Bug) capture StartSnapshot

    CONCAT (
      "ndt5-",
      IF(S2C.ClientIP LIKE "%:%", "IPv6-", "IPv4-"),
      raw.Control.Protocol,
      if (raw.Control.Protocol = 'plain',
          CONCAT('-',raw.Control.MessageProtocol),
          '')
    ) AS NDTprotocol,

    -- IsOAM
    ( downloads.S2C.ClientIP IN
         -- TODO(m-lab/etl/issues/893): move to parser configuration.
        ( "35.193.254.117", -- script-exporter VMs in GCE, sandbox.
          "35.225.75.192", -- script-exporter VM in GCE, staging.
          "35.192.37.249", -- script-exporter VM in GCE, oti.
          "23.228.128.99", "2605:a601:f1ff:fffe::99", -- ks addresses.
          "45.56.98.222", "2600:3c03::f03c:91ff:fe33:819", -- eb addresses.
          "35.202.153.90", "35.188.150.110" -- Static IPs from GKE VMs for e2e tests.
        )
     ) AS IsOAM, -- refactored ToDO move to a BQ fuction

     -- _IsRFC1918   XXX
     ( (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(downloads.S2C.ClientIP),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(downloads.S2C.ClientIP),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(downloads.S2C.ClientIP),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
    ) AS _IsRFC1918, -- TODO does this matter?

    -- IsProduction TODO Check Server Metadata(?)
    REGEXP_CONTAINS(NDTparser.ArchiveURL,
           'mlab[1-3]-[a-z][a-z][a-z][0-9][0-9]') AS IsProduction,

    -- TODO decide which obsolete filters to keep
     -- IsCongested: Any loss implys a netowork bottleneck
    (FinalSnapshot.TCPInfo.TotalRetrans > 0) AS IsCongested,
    -- IsBloated: Final RTT sample twice the minimum and above 1 second means bloated
    ((FinalSnapshot.TCPInfo.RTT > 2*FinalSnapshot.TCPInfo.MinRTT) AND
       (FinalSnapshot.TCPInfo.RTT > 1000)) AS IsBloated,

    TCPparser
  FROM
    -- Use a left join to allow NDT tests without matching tcpinfo rows.
    ndt5downloads AS downloads
    LEFT JOIN tcpinfo AS tcpinfo USING ( date, id ) -- This may exclude a few rows issue:#63
),

-- Standard cols must exactly match the Unified Download Schema
UnifiedDownloadSchema AS (
  SELECT
    id,
    date,
    STRUCT (
      -- NDT unified fields: Upload/Download/RTT/Loss/CCAlg + Geo + ASN
      a.UUID,
      a.TestTime,
      'Download' AS Direction,
      FinalSnapshot.CongestionAlgorithm AS CongestionControl,
      a.MeanThroughputMbps,
      a.MinRTT, -- units are ms
      SAFE_DIVIDE(FinalSnapshot.TCPInfo.BytesRetrans, FinalSnapshot.TCPInfo.BytesSent) AS LossRate
    ) AS a,

    STRUCT (
      'extended_ndt5_downloads' AS viewSource,
      NDTprotocol,
      Control.ClientMetadata AS ClientMetadata,
      Control.ServerMetadata AS ServerMetadata,
      [NDTparser, TCPparser] AS Sources
    ) AS metadata,

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

    STRUCT (
      S2C.ClientIP AS IP, -- TODO relocate and/or redact this field
      S2C.ClientPort AS Port, -- TODO relocate this field
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
      S2C.ServerIP AS IP, -- TODO relocate this field
      S2C.ServerPort AS Port, -- TODO relocate this field
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

    PreComputeNDT5 AS _internal202205  -- Not stable and subject to breaking changes

  FROM PreComputeNDT5
)

SELECT * FROM UnifiedDownloadSchema
