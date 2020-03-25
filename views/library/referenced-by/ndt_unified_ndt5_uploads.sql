--
-- NDT5 upload data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab Unified Standard Views.
--
-- This view is only intended to accessed by a MLab Standard views: breaking changes
-- here will be offset by changes to the Standard views.
--
-- Researchers accessing this view to construct their own samples may
-- need to track changes to MLab Unified Standard Views.
--

WITH ndt5uploads AS (
  SELECT partition_date, result.C2S,
  (result.C2S.Error != "") AS b_HasError,
  TIMESTAMP_DIFF(result.C2S.EndTime, result.C2S.StartTime, MICROSECOND) AS connection_duration
  FROM   `{{.ProjectID}}.ndt.ndt5`
  -- Limit results with C2S results and without any error.
  WHERE  result.C2S IS NOT NULL AND result.C2S.Error = ""
),

tcpinfo AS (
  SELECT * EXCEPT (snapshots)
  FROM `{{.ProjectID}}.ndt.tcpinfo`
),

PreCleanTCPinfo AS (
  SELECT
    uploads.*, tcpinfo.Client, tcpinfo.Server,
    tcpinfo.FinalSnapshot AS FinalSnapshot,
    -- Receiver side can not compute b_HasLoss
    -- Receiver side can not directly compute b_HasBloat
    (
      uploads.C2S.ClientIP IN
	("45.56.98.222", "35.192.37.249", "35.225.75.192", "23.228.128.99",
	"2600:3c03::f03c:91ff:fe33:819",  "2605:a601:f1ff:fffe::99")
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(uploads.C2S.ServerIP),
		8) = NET.IP_FROM_STRING("10.0.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(uploads.C2S.ServerIP),
		12) = NET.IP_FROM_STRING("172.16.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(uploads.C2S.ServerIP),
		16) = NET.IP_FROM_STRING("192.168.0.0"))
    ) AS b_OAM  -- Data is not from valid clients
 FROM
    -- Use a left join to allow NDT test without matching tcpinfo rows.
    ndt5uploads AS uploads
    LEFT JOIN tcpinfo
    ON
#    uploads.partition_date = tcpinfo.partition_date AND -- why does this exclude rows?
     uploads.C2S.UUID = tcpinfo.UUID
),

NDT5UploadModels AS (
  SELECT
    partition_date as test_date,
    STRUCT (
      -- NDT unified fields: Upload/Download/RTT/Loss/CCAlg + Geo + ASN
      C2S.UUID,
      C2S.StartTime AS TestTime,
      FinalSnapshot.CongestionAlgorithm AS CongestionControl,
      C2S.MeanThroughputMbps,
      FinalSnapshot.TCPInfo.MinRTT, -- Sender's MinRTT
      Null AS LossRate,  -- Receiver can not disambiguate reordering and loss
      "ndt5.tcp_info" AS ToolStack
    ) AS a,
    STRUCT (
      ( -- Many Filters are built into the parser
      	NOT b_OAM AND NOT b_HasError
      	AND FinalSnapshot.TCPInfo.BytesReceived IS NOT NULL
      	AND FinalSnapshot.TCPInfo.BytesReceived >= 8192
	AND connection_duration BETWEEN 9000000 AND 60000000
      ) AS row_valid_best,
      ( -- Losses > 0 make no sense for C2S
      	NOT b_OAM AND NOT b_HasError
      	AND FinalSnapshot.TCPInfo.BytesReceived IS NOT NULL
AND FinalSnapshot.TCPInfo.BytesReceived >= 8192
	AND connection_duration BETWEEN 9000000 AND 60000000
      ) AS row_valid_2019  -- Same as row_valid_best
    ) AS b,
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
	CAST (Client.Network.Systems[OFFSET(0)].ASNs[OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS client,
    STRUCT (
      C2S.ServerIP AS IP,
      C2S.ServerPort AS Port,
      Server.Geo,
      STRUCT(
	CAST (Server.Network.Systems[OFFSET(0)].ASNs[OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS server,
    STRUCT (
    	b_OAM,
    	FinalSnapshot
    ) AS RawNDT5

  FROM PreCleanTCPinfo
)

SELECT * FROM NDT5UploadModels
