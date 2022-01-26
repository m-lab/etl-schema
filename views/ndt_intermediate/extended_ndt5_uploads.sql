--
-- NDT5 upload data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab standard Unified Views.
--
-- Anything here that is not visible in the unified views is subject to
-- breaking changes.  Use with caution!
--
-- See the documentation on creating custom unified views.
--

WITH ndt5uploads AS (
  SELECT date, parser, raw.C2S, client, server,
  (raw.C2S.Error != "") AS IsErrored,
  TIMESTAMP_DIFF(raw.C2S.EndTime, raw.C2S.StartTime, MICROSECOND) AS connection_duration
  FROM   `{{.ProjectID}}.ndt.ndt5` -- TODO move to intermediate_ndt
  -- Limit to valid C2S results
  WHERE  raw.C2S IS NOT NULL
  AND raw.C2S.UUID NOT IN ( '', 'ERROR_DISCOVERING_UUID' )
),

tcpinfo AS (
  SELECT * EXCEPT (snapshots)
  FROM `{{.ProjectID}}.ndt.tcpinfo` -- TODO move to intermediate_ndt
),

PreCleanNDT5 AS (
  SELECT
    uploads.*,
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
      OR REGEXP_EXTRACT(uploads.parser.ArchiveURL, '(mlab[1-4])-[a-z][a-z][a-z][0-9][0-9t]') = 'mlab4'
    ) AS IsOAM,  -- Data is not from valid clients
    tcpinfo.ParseInfo AS TCPparser,
    uploads.parser AS NDT5parser,
  FROM
    -- Use a left join to allow NDT test without matching tcpinfo rows.
    ndt5uploads AS uploads
    LEFT JOIN tcpinfo
    ON
#     uploads.date = tcpinfo.partition_date AND -- This may exclude a few rows issue:#63
      uploads.C2S.UUID = tcpinfo.UUID
),

NDT5UploadModels AS (
  SELECT
    C2S.UUID AS id,
    date,
    STRUCT (
      -- NDT unified fields: Upload/Download/RTT/Loss/CCAlg + Geo + ASN
      C2S.UUID,
      C2S.StartTime AS TestTime,
      '' AS CongestionControl, -- https://github.com/m-lab/etl-schema/issues/95
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
      client.Geo,
      client.Network
    ) AS client,
    STRUCT (
      C2S.ServerIP AS IP,
      C2S.ServerPort AS Port,
      server.Site,
      server.Machine,
      server.Geo,
      server.Network
    ) AS server,
    PreCleanNDT5 AS _internal202010  -- Not stable and subject to breaking changes
  FROM PreCleanNDT5
)

SELECT * FROM NDT5UploadModels
