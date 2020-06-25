--
-- NDT5 download data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab Unified Standard Views.
--
-- This view is only intended to accessed by a MLab Standard views: breaking changes
-- here will be offset by changes to the Published Standard views.
--
-- Anything here not visible in a standard view is subject to breaking changes.
--

WITH ndt5downloads AS (
  SELECT partition_date, ParseInfo, result.S2C,
  (result.S2C.Error != "") AS IsErrored,
  TIMESTAMP_DIFF(result.S2C.EndTime, result.S2C.StartTime, MICROSECOND) AS connection_duration
  FROM   `mlab-oti.ndt.ndt5`
  -- Limit to valid S2C results
  WHERE result.S2C IS NOT NULL
  AND result.S2C.UUID IS NOT NULL
  AND result.S2C.UUID NOT IN ( '', 'ERROR_DISCOVERING_UUID' )
),

tcpinfo AS (
  SELECT * EXCEPT (snapshots)
  FROM `mlab-oti.ndt.tcpinfo`
),

PreCleanNDT5 AS (
  SELECT
    downloads.*, tcpinfo.Client, tcpinfo.Server,
    tcpinfo.FinalSnapshot AS FinalSnapshot,
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
    ) AS IsOAM,  -- Data is not from valid clients
    tcpinfo.ParseInfo AS TCPparser,
    downloads.ParseInfo AS NDT5parser,
  FROM
    -- Use a left join to allow NDT test without matching tcpinfo rows.
    ndt5downloads AS downloads
    LEFT JOIN tcpinfo
    ON
#     downloads.partition_date = tcpinfo.partition_date AND -- This may exclude a few rows issue:#63
      downloads.S2C.UUID = tcpinfo.UUID
),

NDT5DownloadModels AS (
  SELECT
    S2C.UUID AS id,
    partition_date as test_date, -- rename to date
    STRUCT (
      -- NDT unified fields: Upload/Download/RTT/Loss/CCAlg + Geo + ASN
      S2C.UUID,
      S2C.StartTime AS TestTime,
      FinalSnapshot.CongestionAlgorithm AS CongestionControl,
      S2C.MeanThroughputMbps AS MeanThroughputMbps,
      S2C.MinRTT/1000000.0 AS MinRTT, -- units are ms
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
      Client.Geo,
      STRUCT(
        -- NOTE: Omit the NetBlock field because neither web100 nor ndt5 tables
        -- includes this information yet.
        -- NOTE: Select the first ASN b/c standard columns defines a single field.
        CAST (Client.Network.Systems[OFFSET(0)].ASNs[OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS client,
    STRUCT (
      S2C.ServerIP AS IP,
      S2C.ServerPort AS Port,
      REGEXP_EXTRACT(ParseInfo.TaskFileName,
            'mlab[1-4]-([a-z][a-z][a-z][0-9][0-9t])') AS Site, -- e.g. lga02
      REGEXP_EXTRACT(ParseInfo.TaskFileName,
            '(mlab[1-4])-[a-z][a-z][a-z][0-9][0-9t]') AS Machine, -- e.g. mlab1
      Server.Geo,
      STRUCT(
        CAST (Server.Network.Systems[OFFSET(0)].ASNs[OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS server,
    PreCleanNDT5 AS _internal202006  -- Not stable and subject to breaking changes
  FROM PreCleanNDT5
)

SELECT * FROM NDT5DownloadModels
