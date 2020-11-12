--
-- NDT5 upload data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab Unified Standard Views.
--
-- This view is only intended to accessed by a MLab Standard views: breaking changes
-- here will be offset by changes to the Published Standard views.
--
-- Anything here not visible in a standard view is subject to breaking changes.
--

WITH ndt5uploads AS (
  SELECT partition_date, ParseInfo, result.C2S,
  (result.C2S.Error != "") AS IsErrored,
  TIMESTAMP_DIFF(result.C2S.EndTime, result.C2S.StartTime, MICROSECOND) AS connection_duration
  FROM   `mlab-oti.ndt.ndt5`
  -- Limit to valid C2S results
  WHERE  result.C2S IS NOT NULL
  AND result.C2S.UUID NOT IN ( '', 'ERROR_DISCOVERING_UUID' )
),

tcpinfo AS (
  SELECT * EXCEPT (snapshots)
  FROM `mlab-oti.ndt.tcpinfo`
),

PreCleanNDT5 AS (
  SELECT
    uploads.*, tcpinfo.Client, tcpinfo.Server,
    tcpinfo.FinalSnapshot AS FinalSnapshot,
    -- Receiver side can not compute IsCongested
    -- Receiver side can not directly compute IsBloated
    ( uploads.C2S.ClientIP IN
         -- TODO(m-lab/etl/issues/893): move to parser configuration.
        ( "35.193.254.117", -- script-exporter VMs in GCE, sandbox.
          "35.225.75.192", -- script-exporter VM in GCE, staging.
          "35.192.37.249", -- script-exporter VM in GCE, oti.
          "23.228.128.99", "2605:a601:f1ff:fffe::99", -- ks addresses.
          "45.56.98.222", "2600:3c03::f03c:91ff:fe33:819", -- eb addresses.
          "35.202.153.90", "35.188.150.110" -- Static IPs from GKE VMs for e2e tests.
        )
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(uploads.C2S.ServerIP),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(uploads.C2S.ServerIP),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(uploads.C2S.ServerIP),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
    ) AS IsOAM,  -- Data is not from valid clients
    tcpinfo.ParseInfo AS TCPparser,
    uploads.ParseInfo AS NDT5parser,
  FROM
    -- Use a left join to allow NDT test without matching tcpinfo rows.
    ndt5uploads AS uploads
    LEFT JOIN tcpinfo
    ON
#     uploads.partition_date = tcpinfo.partition_date AND -- This may exclude a few rows issue:#63
      uploads.C2S.UUID = tcpinfo.UUID
),

NDT5UploadModels AS (
  SELECT
    C2S.UUID AS id,
    partition_date as test_date, -- rename to date
    STRUCT (
      -- NDT unified fields: Upload/Download/RTT/Loss/CCAlg + Geo + ASN
      C2S.UUID,
      C2S.StartTime AS TestTime,
      FinalSnapshot.CongestionAlgorithm AS CongestionControl,
      C2S.MeanThroughputMbps AS MeanThroughputMbps,
      FinalSnapshot.TCPInfo.MinRTT/1000.0 AS MinRTT, -- Sender's MinRTT (ms)
      Null AS LossRate  -- Receiver can not disambiguate reordering and loss
    ) AS a,
    STRUCT (
     "tcpinfo" AS _Instruments -- THIS WILL CHANGE
    ) AS node,
    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      (  -- No C2S test for client vs network bottleneck
        NOT IsOAM AND NOT IsErrored
        AND FinalSnapshot.TCPInfo.BytesReceived IS NOT NULL
        AND FinalSnapshot.TCPInfo.BytesReceived >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
      ) AS IsValidBest,
      (  -- No C2S test for client vs network bottleneck
        NOT IsOAM AND NOT IsErrored
        AND FinalSnapshot.TCPInfo.BytesReceived IS NOT NULL
        AND FinalSnapshot.TCPInfo.BytesReceived >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
      ) AS IsValid2019  -- Same as row_valid_best
    ) AS filter,
    -- NOTE: standard columns for views exclude the parseInfo struct because
    -- multiple tables are used to create a derived view. Users that want the
    -- underlying parseInfo values should refer to the corresponding tables
    -- using the shared UUID.
    STRUCT (
      C2S.ClientIP AS IP,
      C2S.ClientPort AS Port,
      Client.Geo,
      STRUCT(
        -- NOTE: Omit the NetBlock field because neither web100 nor ndt5 tables
        -- includes this information yet.
        -- NOTE: Select the first ASN b/c standard columns defines a single field.
        CAST (Client.Network.Systems[SAFE_OFFSET(0)].ASNs[SAFE_OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS client,
    STRUCT (
      C2S.ServerIP AS IP,
      C2S.ServerPort AS Port,
      REGEXP_EXTRACT(ParseInfo.TaskFileName,
            'mlab[1-4]-([a-z][a-z][a-z][0-9][0-9t])') AS Site, -- e.g. lga02
      REGEXP_EXTRACT(ParseInfo.TaskFileName,
            '(mlab[1-4])-[a-z][a-z][a-z][0-9][0-9t]') AS Machine, -- e.g. mlab1
      Server.Geo,
      STRUCT(
        CAST (Server.Network.Systems[SAFE_OFFSET(0)].ASNs[SAFE_OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS server,
    PreCleanNDT5 AS _internal202006  -- Not stable and subject to breaking changes
  FROM PreCleanNDT5
)

SELECT * FROM NDT5UploadModels
