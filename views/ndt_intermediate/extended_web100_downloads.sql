--
-- Legacy NDT/web100 downloads data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab standard Unified Views.
--
-- Anything here that is not visible in the unified views is subject to
-- breaking changes.  Use with caution!
--
-- See the documentation on creating custom unified views.
--

WITH

PreComputeWeb100 AS (
  SELECT
    *,
    -- TODO: restore when blacklist flags (or alternate name) is restored.
    -- (blacklist_flags IS NOT NULL and blacklist_flags != 0
    --   OR anomalies.blacklist_flags IS NOT NULL )
    False AS IsPlatformAnomaly,
    ( raw.web100.snap.Duration IS NOT NULL
      AND raw.web100.snap.State IS NOT NULL
      AND raw.connection.server_ip IS NOT NULL
      AND raw.connection.client_ip IS NOT NULL
      AND raw.web100.snap.SndLimTimeRwin IS NOT NULL
      AND raw.web100.snap.SndLimTimeCwnd IS NOT NULL
      AND raw.web100.snap.SndLimTimeSnd IS NOT NULL
    ) AS IsComplete,
    False AS IsErrored,

    raw.web100.snap.Duration AS connection_duration, -- SYN to final snap time (may include SSL)
    (raw.web100.snap.SndLimTimeRwin +
         raw.web100.snap.SndLimTimeCwnd +
         raw.web100.snap.SndLimTimeSnd) AS measurement_duration, -- Time transfering data

    -- Modernize Client and Server Metadata
    [ -- STRUCT('client_af', raw.connection.client_af ) -- int not supported
      STRUCT('client_application' AS Name, raw.connection.client_application AS Value),
      STRUCT('client_browser', raw.connection.client_browser),
      STRUCT('client_hostname', raw.connection.client_hostname),
      STRUCT('client_ip', raw.connection.client_ip),
      STRUCT('client_kernel_version', raw.connection.client_kernel_version),
      STRUCT('client_os', raw.connection.client_os),
      STRUCT('client_version', raw.connection.client_version)
    ] AS ClientMetadata,
    [ STRUCT('server_hostname' AS name, raw.connection.server_hostname AS Value),
      STRUCT('server_ip', raw.connection.server_ip),
      STRUCT('server_kernel_version', raw.connection.server_kernel_version)
    ] AS ServerMetadata,

    CONCAT(
      "Web100",
      IF(raw.connection.client_ip LIKE "%:%", "-IPv6", "-IPv4"),
      IF(raw.connection.websockets,
        if(raw.connection.tls, "-WSS", "-WS"),
        if(raw.connection.tls, "-SSL", "-PLAIN"))
    ) AS NDTprotocol,

    -- IsOAM.  Note that this list only include early OAM devices
    (raw.web100.connection_spec.remote_ip IN
          ("45.56.98.222", "35.192.37.249", "35.225.75.192", "23.228.128.99",
          "2600:3c03::f03c:91ff:fe33:819", "2605:a601:f1ff:fffe::99"
    ) ) AS IsOAM, -- TODO Generalize

     -- _IsRFC1918  XXX deprecate?
    ( (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.web100.connection_spec.remote_ip),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.web100.connection_spec.remote_ip),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.web100.connection_spec.remote_ip),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
     ) AS _IsRFC1918, -- TODO does this matter?

     -- IsProduction TODO Check Server Metadata(?)
    REGEXP_CONTAINS(parser.ArchiveURL,
           'mlab[1-3]-[a-z][a-z][a-z][0-9][0-9]') AS IsProduction,

     -- Obsolete IsCongested and IsBloated, used by IsValid2021
     raw.web100.snap.OctetsRetrans > 0 AS IsCongested,
     (  raw.web100.snap.SmoothedRTT > 2*raw.web100.snap.MinRTT AND
        raw.web100.snap.SmoothedRTT > 1000 ) AS IsBloated,

     parser AS Web100parser

#  FROM `{{.ProjectID}}.ndt_raw.web100_legacy` -- TODO move to intermediate_ndt
   FROM `mlab-oti.ndt.web100_static`  -- XXX debugging code
),

-- Standard cols must exactly match the Unified Download Schema
UnifiedDownloadSchema AS (
  SELECT
    id,
    date,
    -- Struct a models various TCP behaviors
    STRUCT(
      a.UUID,
      a.TestTime,
      'Download' AS Direction,
      "reno" AS CongestionControl,
      SAFE_DIVIDE(raw.web100.snap.HCThruOctetsAcked * 8.0, measurement_duration) AS MeanThroughputMbps,
      raw.web100.snap.MinRTT * 1.0 AS MinRTT,
      SAFE_DIVIDE(raw.web100.snap.SegsRetrans, raw.web100.snap.SegsOut) AS LossRate
    ) AS a,

    STRUCT (
      'extended_web100_downloads' AS viewSource,
      NDTprotocol,
      ClientMetadata,
      ServerMetadata,
      [ parser ] AS Sources
    ) AS Metadata,

    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      IsComplete,
      IsProduction,
      IsErrored,
      IsOAM,
      _IsRFC1918, --XXX
      IsPlatformAnomaly,
      Null AS WhatPlatformAnomaly,
      raw.web100.snap.HCThruOctetsAcked < 8192 AS IsShort,
      measurement_duration < 9000000 AS IsAborted,
      measurement_duration > 60000000 AS IsHung,
      IsCongested AS _IsCongested, -- XXX Deprecate?
      IsBloated AS _IsBloated -- XXX Deprecate?
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
        "", -- mask out region.
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
      REGEXP_EXTRACT(raw.connection.server_hostname, 'mlab[1-4].([a-z][a-z][a-z][0-9][0-9t])') AS Site,
      REGEXP_EXTRACT(raw.connection.server_hostname, '(mlab[1-4])') AS Machine,
      -- TODO(https://github.com/m-lab/etl/issues/1069): eliminate region mask once parser does this.
      STRUCT(
        server.Geo.ContinentCode,
        server.Geo.CountryCode,
        server.Geo.CountryCode3,
        server.Geo.CountryName,
        "", -- mask out region.
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
    PreComputeWeb100 AS _internal202205  -- Not stable and subject to breaking changes
  FROM PreComputeWeb100
)

SELECT * FROM UnifiedDownloadSchema
