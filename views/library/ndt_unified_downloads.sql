#StandardSQL
-- ndt_unified_downloads is a `library` view designed to combine all historical
-- NDT download measurements. The resulting schema uses field names defined by
-- the Standard Top-level Columns design. This schema should receive long term
-- support.
--
-- Note on design: ndt5 joins with the tcpinfo table in order to collect the
-- Server & Client geo information. Once the uuid-annotator is deloyed, the geo
-- annotations should come from the annotation tables, and the LossRate metric
-- should be saved by ndt5 natively. As well, once ndt5 parser outputs rows that
-- use standard columns, this view becomes much simpler: join ndt5 with
-- annotations, take the `ndt5.a` column and combine with annotation.server &
-- annotation.client rows.

WITH ndt5downloads AS (
  SELECT
    partition_date,
    result.S2C
  FROM
    `measurement-lab.ndt.ndt5`
  WHERE
    result.S2C IS NOT NULL
    AND result.S2C.Error = "" -- Limit results to those without any error.
),

tcpinfo AS (
  SELECT * EXCEPT (snapshots)
  FROM `measurement-lab.ndt.tcpinfo`
),

ndt5_tcpinfo_joined AS (
  SELECT
    downloads.*, tcpinfo.Client, tcpinfo.Server, tcpinfo.FinalSnapshot.TCPInfo AS TCPInfo,
  FROM
    -- Use a left join to allow NDT test without matching tcpinfo rows.
    ndt5downloads AS downloads LEFT JOIN tcpinfo
    ON
     downloads.partition_date = tcpinfo.partition_date AND
     downloads.S2C.UUID = tcpinfo.UUID AND
     tcpinfo.FinalSnapshot.TCPInfo.TotalRetrans > 0
),

StandardNDT5 AS (
  SELECT
    partition_date as test_date,
    STRUCT (
      -- NDT unified fields: Upload/Download/RTT/Loss/CCAlg + Geo + ASN
      S2C.UUID,
      S2C.StartTime AS TestTime,
      "cubic" AS CongestionControl,
      S2C.MeanThroughputMbps,
      S2C.MinRTT/1000000000 AS MinRTT,
      TCPInfo.BytesRetrans / TCPInfo.BytesSent AS LossRate
    ) AS a,
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
        -- inclues this information yet.
        -- NOTE: Select the first ASN b/c stanard columns defines a single field.
        CAST (Client.Network.Systems[OFFSET(0)].ASNs[OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS client,
    STRUCT (
      S2C.ServerIP AS IP,
      S2C.ServerPort AS Port,
      Server.Geo,
      STRUCT(
        CAST (Server.Network.Systems[OFFSET(0)].ASNs[OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS server,
  FROM ndt5_tcpinfo_joined
),

StandardWeb100 AS (
  SELECT
    partition_date as test_date,
    STRUCT(
      CONCAT(
        web100_log_entry.connection_spec.local_ip,
        CAST (web100_log_entry.connection_spec.local_port AS STRING),
        web100_log_entry.connection_spec.remote_ip,
        CAST (web100_log_entry.connection_spec.remote_port AS STRING),
        CAST (partition_date AS STRING)
      ) AS UUID,
      log_time AS TestTime,
      "reno" AS CongestionControl,
      alpha.mean_download_throughput_mbps AS MeanThroughputMbps,
      web100_log_entry.snap.MinRTT/1000000 AS MinRTT,
      web100_log_entry.snap.SegsRetrans / web100_log_entry.snap.SegsOut AS LossRate
    ) AS a,
    STRUCT (
      web100_log_entry.connection_spec.remote_ip AS IP,
      web100_log_entry.connection_spec.remote_port AS Port,
      STRUCT(
        -- NOTE: it's necessary to enumerate each field because the new Server.Geo
        -- fields are in a different order. Here reorder the web100 fields because
        -- we accept the newer tables as the canonical ordering.
        connection_spec.client_geolocation.continent_code,
        connection_spec.client_geolocation.country_code,
        connection_spec.client_geolocation.country_code3,
        connection_spec.client_geolocation.country_name,
        connection_spec.client_geolocation.region,
        connection_spec.client_geolocation.metro_code,
        connection_spec.client_geolocation.city,
        connection_spec.client_geolocation.area_code,
        connection_spec.client_geolocation.postal_code,
        connection_spec.client_geolocation.latitude,
        connection_spec.client_geolocation.longitude,
        connection_spec.client_geolocation.radius
      ) AS Geo,
      STRUCT(
        connection_spec.client.network.asn AS ASNumber
      ) AS Network
    ) AS client,
    STRUCT (
      web100_log_entry.connection_spec.local_ip AS IP,
      web100_log_entry.connection_spec.local_port AS Port,
      STRUCT(
        connection_spec.server_geolocation.continent_code,
        connection_spec.server_geolocation.country_code,
        connection_spec.server_geolocation.country_code3,
        connection_spec.server_geolocation.country_name,
        connection_spec.server_geolocation.region,
        connection_spec.server_geolocation.metro_code,
        connection_spec.server_geolocation.city,
        connection_spec.server_geolocation.area_code,
        connection_spec.server_geolocation.postal_code,
        connection_spec.server_geolocation.latitude,
        connection_spec.server_geolocation.longitude,
        connection_spec.server_geolocation.radius
      ) AS Geo,
      STRUCT(
        connection_spec.server.network.asn AS ASNumber
      ) AS Network
    ) AS server,
  FROM
    `measurement-lab.ndt.downloads`
)

-- Export the result of the union of both tables.
SELECT * FROM StandardWeb100
UNION ALL
SELECT * FROM StandardNDT5
