--
-- NDT5 download data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab Unified Standard Views.
--
-- Anything here that is not visible in the unified views is subject to
-- breaking changes.  Use with caution!
--
-- See the documentation on creating custom unified views.
--

WITH ndt5downloads AS (
  SELECT date, parser, raw.S2C, client, server,
  (raw.S2C.Error IS NOT NULL AND raw.S2C.Error != "") AS IsErrored,
  TIMESTAMP_DIFF(raw.S2C.EndTime, raw.S2C.StartTime, MICROSECOND) AS connection_duration
  FROM   `{{.ProjectID}}.ndt.ndt5` -- TODO move to intermediate_ndt
  -- Limit to valid S2C results
  WHERE raw.S2C IS NOT NULL
  AND raw.S2C.UUID IS NOT NULL
  AND raw.S2C.UUID NOT IN ( '', 'ERROR_DISCOVERING_UUID' )
),

tcpinfo AS (
  SELECT * FROM `{{.ProjectID}}.ndt_raw.tcpinfo` -- TODO move to intermediate_ndt
),

PreCleanNDT5 AS (
  SELECT
    downloads.*,
    tcpinfo.a.FinalSnapshot AS FinalSnapshot,
    -- Any loss implys a netowork bottleneck
    (FinalSnapshot.TCPInfo.TotalRetrans > 0) AS IsCongested,
    -- Final RTT sample twice the minimum and above 1 second means bloated
    ((FinalSnapshot.TCPInfo.RTT > 2*FinalSnapshot.TCPInfo.MinRTT) AND
       (FinalSnapshot.TCPInfo.RTT > 1000)) AS IsBloated,
    (
      downloads.S2C.ClientIP IN
         -- TODO(m-lab/etl/issues/893): move to parser configuration.
        ( "35.193.254.117", -- script-exporter VMs in GCE, sandbox.
          "35.225.75.192", -- script-exporter VM in GCE, staging.
          "35.192.37.249", -- script-exporter VM in GCE, oti.
          "23.228.128.99", "2605:a601:f1ff:fffe::99", -- ks addresses.
          "45.56.98.222", "2600:3c03::f03c:91ff:fe33:819", -- eb addresses.
          "35.202.153.90", "35.188.150.110" -- Static IPs from GKE VMs for e2e tests.
        )
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(downloads.S2C.ServerIP),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(downloads.S2C.ServerIP),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(downloads.S2C.ServerIP),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
      OR REGEXP_EXTRACT(downloads.parser.ArchiveURL, '(mlab[1-4])-[a-z][a-z][a-z][0-9][0-9t]') = 'mlab4'
    ) AS IsOAM,  -- Data is not from valid clients
    tcpinfo.parser AS TCPparser,
    downloads.parser AS NDT5parser,
  FROM
    -- Use a left join to allow NDT test without matching tcpinfo rows.
    ndt5downloads AS downloads
    LEFT JOIN tcpinfo
    ON
      downloads.date = tcpinfo.date AND -- This may exclude a few rows issue:#63
      downloads.id = tcpinfo.id
),

NDT5DownloadModels AS (
  SELECT
    id,
    date,
    STRUCT (
      -- NDT unified fields: Upload/Download/RTT/Loss/CCAlg + Geo + ASN
      a.UUID,
      a.TestTime,
      FinalSnapshot.CongestionAlgorithm AS CongestionControl,
      a.MeanThroughputMbps AS MeanThroughputMbps,
      a.MinRTT, -- units are ms
      SAFE_DIVIDE(FinalSnapshot.TCPInfo.BytesRetrans, FinalSnapshot.TCPInfo.BytesSent) AS LossRate
    ) AS a,
    STRUCT (
     "tcpinfo" AS _Instruments -- THIS WILL CHANGE
    ) AS node,
    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      (
        NOT IsOAM AND NOT IsErrored
        AND FinalSnapshot.TCPInfo.BytesAcked IS NOT NULL
        AND FinalSnapshot.TCPInfo.BytesAcked >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
        AND ( IsCongested OR IsBloated ) -- Loss or excess queueing indicates congestion
      ) AS IsValidBest,
      (
        NOT IsOAM AND NOT IsErrored
        AND FinalSnapshot.TCPInfo.BytesAcked IS NOT NULL
        AND FinalSnapshot.TCPInfo.BytesAcked >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
        AND ( IsCongested ) -- Only consides loss as a congestion signal
      ) AS IsValid2019
    ) AS filter,
    -- NOTE: standard columns for views exclude the parseInfo struct because
    -- multiple tables are used to create a derived view. Users that want the
    -- underlying parseInfo values should refer to the corresponding tables
    -- using the shared UUID.
    STRUCT (
      S2C.ClientIP AS IP,
      S2C.ClientPort AS Port,
      -- TODO(https://github.com/m-lab/etl/issues/1069): eliminate region mask once parser does this.
      STRUCT(
        client.Geo.ContinentCode,
        client.Geo.CountryCode,
        client.Geo.CountryCode3,
        client.Geo.CountryName,
        NULL as Region, -- mask out region.
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
      S2C.ServerIP AS IP,
      S2C.ServerPort AS Port,
      server.Site,
      server.Machine,
      -- TODO(https://github.com/m-lab/etl/issues/1069): eliminate region mask once parser does this.
      STRUCT(
        server.Geo.ContinentCode,
        server.Geo.CountryCode,
        server.Geo.CountryCode3,
        server.Geo.CountryName,
        NULL as Region, -- mask out region.
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
    PreCleanNDT5 AS _internal202201  -- Not stable and subject to breaking changes
  FROM PreCleanNDT5
)

SELECT * FROM NDT5DownloadModels
