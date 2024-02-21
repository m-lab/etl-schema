--
-- NDT5 upload data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab standard Unified Views.
--
-- Anything here that is not visible in the unified views is subject to
-- breaking changes.  Use with caution!
--
-- See the documentation on creating custom unified views.
--

WITH

ndt5uploads AS (
  SELECT id, date, a, parser NDTparser, raw.C2S, raw, client, server,
    (raw.C2S.Error IS NOT NULL AND raw.C2S.Error != "") AS IsError,
    TIMESTAMP_DIFF(raw.C2S.EndTime, raw.C2S.StartTime, MILLISECOND)*1.0 AS test_duration
  FROM   `{{.ProjectID}}.ndt.ndt5`
  -- Limit to valid C2S results
  WHERE  raw.C2S IS NOT NULL
  AND raw.C2S.UUID IS NOT NULL
  AND raw.C2S.UUID NOT IN ( '', 'ERROR_DISCOVERING_UUID' )  -- TODO(P3) clear IsComplete instead
),

tcpinfo AS (
  SELECT * EXCEPT(a, parser, raw),
    a.FinalSnapshot,
    parser AS TCPparser,
  FROM `{{.ProjectID}}.ndt_raw.tcpinfo`
),

PreComputeNDT5 AS (
  SELECT
    uploads.*,
    FinalSnapshot, raw.Control,

    FinalSnapshot IS NOT NULL AS IsComplete, -- Not Missing any key fields

    -- Protocol
    CONCAT ("ndt5-",
      IF(C2S.ClientIP LIKE "%:%", "IPv6-", "IPv4-"),
      raw.Control.Protocol,
      if (raw.Control.Protocol = 'plain',
          CONCAT('-',raw.Control.MessageProtocol),
          '') ) AS Protocol,

    -- TODO(https://github.com/m-lab/etl/issues/893) generalize IsOAM
    ( uploads.C2S.ClientIP IN
        ( "35.193.254.117", -- script-exporter VMs in GCE, sandbox.
          "35.225.75.192", -- script-exporter VM in GCE, staging.
          "35.192.37.249", -- script-exporter VM in GCE, oti.
          "23.228.128.99", "2605:a601:f1ff:fffe::99", -- ks addresses.
          "45.56.98.222", "2600:3c03::f03c:91ff:fe33:819", -- eb addresses.
          "35.202.153.90", "35.188.150.110" -- Static IPs from GKE VMs for e2e tests.
        ) ) AS IsOAM,

    -- TODO(https://github.com/m-lab/k8s-support/issues/668) deprecate? _IsRFC1918
    ( (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(uploads.C2S.ServerIP),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(uploads.C2S.ServerIP),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(uploads.C2S.ServerIP),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
    ) AS _IsRFC1918,

    REGEXP_CONTAINS(NDTparser.ArchiveURL,
      'mlab[1-3]-[a-z][a-z][a-z][0-9][0-9]') AS IsProduction,

    TCPparser
  FROM
    -- Use a left join to allow NDT tests without matching tcpinfo rows.
    ndt5uploads AS uploads
    LEFT JOIN tcpinfo AS tcpinfo USING ( date, id ) -- This may exclude a few rows issue:#63
),

-- Standard cols must exactly match the Unified Upload Schema
UnifiedUploadSchema AS (
  SELECT
    id,
    date,
    STRUCT (
      a.UUID,
      a.TestTime,
      'Upload' AS Direction,
      'Unknown' AS CongestionControl, -- https://github.com/m-lab/etl-schema/issues/95
      a.MeanThroughputMbps,
      a.MinRTT,  -- mS
      Null AS LossRate  -- Receiver can not disambiguate reordering and loss
    ) AS a,

    STRUCT (
      'extended_ndt5_uploads' AS View,
      Protocol,
      Control.ClientMetadata AS ClientMetadata,
      Control.ServerMetadata AS ServerMetadata,
      [NDTparser, TCPparser] AS Tables
    ) AS metadata,

    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      IsComplete, -- Not Missing any key fields
      IsProduction,     -- Not mlab4, abc0t, or other pre production servers
      IsError,                  -- Server reported a problem
      IsOAM,               -- internal testing and monitoring
      _IsRFC1918,            -- Not a real client (deprecate?)
      False AS IsPlatformAnomaly, -- FUTURE, No switch discards, etc
      (FinalSnapshot.TCPInfo.BytesReceived < 8192) AS IsSmall, -- not enough data
      (test_duration < 9000.0) AS IsShort,   -- Did not run for enough time
      (test_duration > 60000.0) AS IsLong,    -- Ran for too long
      False AS IsEarlyExit, -- not supported for upload tests
      False AS _IsCongested,
      False AS _IsBloated
    ) AS filter,

    STRUCT (
      -- TODO(https://github.com/m-lab/etl-schema/issues/141) Relocate IP and port
      C2S.ClientIP AS IP,
      C2S.ClientPort AS Port,
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
      -- TODO(https://github.com/m-lab/etl-schema/issues/141) Relocate IP and port
      C2S.ServerIP AS IP,
      C2S.ServerPort AS Port,
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

    PreComputeNDT5 AS _internal202402  -- Not stable and subject to breaking changes

  FROM PreComputeNDT5
)

SELECT * FROM UnifiedUploadSchema
