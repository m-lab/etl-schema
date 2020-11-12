--
-- NDT7 download data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab Unified Standard Views.
--
-- This view is only intended to accessed by a MLab Standard views: breaking changes
-- here will be offset by changes to the Published Standard views.
--
-- Anything here not visible in a standard view is subject to breaking changes.
--

WITH ndt7downloads AS (
  SELECT *,
  raw.Download.ServerMeasurements[SAFE_ORDINAL(ARRAY_LENGTH(raw.Download.ServerMeasurements))] AS lastSample,
# (raw.Download.Error != "") AS IsErrored,  -- TODO ndt-server/issues/317
  False AS IsErrored,
  TIMESTAMP_DIFF(raw.Download.EndTime, raw.Download.StartTime, MICROSECOND) AS connection_duration
 FROM   `mlab-oti.ndt.ndt7` -- TODO move to mlab-oti.intermediate_ndt.joined_ndt7
  -- Limit to valid S2C results
  WHERE raw.Download IS NOT NULL
  AND raw.Download.UUID IS NOT NULL
  AND raw.Download.UUID NOT IN ( '', 'ERROR_DISCOVERING_UUID' )
),

PreCleanNDT7 AS (
  SELECT
    id, date, a, IsErrored, lastsample,
    connection_duration, raw,
    client, server,
    -- Any loss implys a netowork bottleneck
    (lastSample.TCPInfo.TotalRetrans > 0) AS IsCongested,
    -- Final RTT sample twice the minimum and above 1 second means bloated
    ((lastSample.TCPInfo.RTT > 2*lastSample.TCPInfo.MinRTT) AND
       (lastSample.TCPInfo.RTT > 1000)) AS IsBloated,
    ( -- IsOAM
      raw.ClientIP IN
         -- TODO(m-lab/etl/issues/893): move to parser configuration.
        ( "35.193.254.117", -- script-exporter VMs in GCE, sandbox.
          "35.225.75.192", -- script-exporter VM in GCE, staging.
          "35.192.37.249", -- script-exporter VM in GCE, oti.
          "23.228.128.99", "2605:a601:f1ff:fffe::99", -- ks addresses.
          "45.56.98.222", "2600:3c03::f03c:91ff:fe33:819", -- eb addresses.
          "35.202.153.90", "35.188.150.110" -- Static IPs from GKE VMs for e2e tests.
        )
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.ServerIP),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.ServerIP),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.ServerIP),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
    ) AS IsOAM,  -- Data is not from valid clients
    parser AS NDT7parser,
  FROM
    ndt7downloads
),

NDT7DownloadModels AS (
  SELECT
    id,
    date,
    STRUCT (
      a.UUID,
      a.TestTime,
      a.CongestionControl,
      a.MeanThroughputMbps,
      a.MinRTT,  -- mS
      a.LossRate
    ) AS a,
    STRUCT (
     -- "Instruments" is not quite the right concept
     "ndt7" AS _Instruments -- THIS WILL CHANGE
    ) AS node,
    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      (
        NOT IsOAM AND NOT IsErrored
        AND lastSample.TCPInfo.BytesAcked IS NOT NULL
        AND lastSample.TCPInfo.BytesAcked >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
        -- Tests without network bottlenecks are presumed to have bottlenecks elsewhere
        AND ( IsCongested OR IsBloated ) -- Loss or excess queueing indicates congestion
      ) AS IsValidBest,
      (
        NOT IsOAM AND NOT IsErrored
        AND lastSample.TCPInfo.BytesAcked IS NOT NULL
        AND lastSample.TCPInfo.BytesAcked >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
        AND ( IsCongested ) -- Only consides loss as a congestion signal
      ) AS IsValid2019
    ) AS filter,
    -- NOTE: standard columns for views exclude the parseInfo struct because
    -- multiple tables are used to create a derived view. Users that want the
    -- underlying parseInfo values should refer to the corresponding tables
    -- using the shared UUID.
    STRUCT (
      raw.ClientIP AS IP,
      raw.ClientPort AS Port,
      -- TODO reverse this mapping in all views (breaking?)
      STRUCT (  -- Map new geo into older production geo
             client.Geo.ContinentCode, -- aka continent_code,
             client.Geo.CountryCode, -- aka country_code,
             client.Geo.CountryCode3, -- aka country_code3,
             client.Geo.CountryName, -- aka country_name,
             client.Geo.Region, -- aka region,
             -- client.Geo. Subdivision1ISOCode -- OMITED
             -- client.Geo. Subdivision1Name -- OMITED
             -- client.Geo.Subdivision2ISOCode -- OMITED
             -- client.Geo.Subdivision2Name -- OMITED
             client.Geo.MetroCode, -- aka metro_code,
             client.Geo.City, -- aka city,
             client.Geo.AreaCode, -- aka area_code,
             client.Geo.PostalCode, -- aka postal_code,
             client.Geo.Latitude, -- aka latitude,
             client.Geo.Longitude, -- aka longitude,
             client.Geo.AccuracyRadiusKm -- aka radius
             -- client.Geo.Missing -- Future
      ) AS Geo,
      STRUCT(
        -- NOTE: Omit the NetBlock field because neither web100 nor ndt5 tables
        -- includes this information yet.
        -- NOTE: Select the first ASN b/c standard columns defines a single field.
        CAST (Client.Network.Systems[SAFE_OFFSET(0)].ASNs[SAFE_OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS client,
    STRUCT (
      raw.ServerIP AS IP,
      raw.ServerPort AS Port,
      REGEXP_EXTRACT(NDT7parser.ArchiveURL,
            'mlab[1-4]-([a-z][a-z][a-z][0-9][0-9t])') AS Site, -- e.g. lga02
      REGEXP_EXTRACT(NDT7parser.ArchiveURL,
            '(mlab[1-4])-[a-z][a-z][a-z][0-9][0-9t]') AS Machine, -- e.g. mlab1
      -- TODO reverse this mapping in all views (breaking?)
      STRUCT (  -- Map new geo into older production geo
             server.Geo.ContinentCode, -- aka continent_code,
             server.Geo.CountryCode, -- aka country_code,
             server.Geo.CountryCode3, -- aka country_code3,
             server.Geo.CountryName, -- aka country_name,
             server.Geo.Region, -- aka region,
             -- server.Geo. Subdivision1ISOCode -- OMITED
             -- server.Geo. Subdivision1Name -- OMITED
             -- server.Geo.Subdivision2ISOCode -- OMITED
             -- server.Geo.Subdivision2Name -- OMITED
             server.Geo.MetroCode, -- aka metro_code,
             server.Geo.City, -- aka city,
             server.Geo.AreaCode, -- aka area_code,
             server.Geo.PostalCode, -- aka postal_code,
             server.Geo.Latitude, -- aka latitude,
             server.Geo.Longitude, -- aka longitude,
             server.Geo.AccuracyRadiusKm -- aka radius
             -- server.Geo.Missing -- Future
      ) AS Geo,
      STRUCT(
        CAST (Server.Network.Systems[SAFE_OFFSET(0)].ASNs[SAFE_OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS server,
    date AS test_date,
    PreCleanNDT7 AS _internal202010  -- Not stable and subject to breaking changes

  FROM PreCleanNDT7
)

SELECT * FROM NDT7DownloadModels
