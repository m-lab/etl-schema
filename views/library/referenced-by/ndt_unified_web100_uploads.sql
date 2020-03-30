--
-- Legacy NDT/web100 upload data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab Unified Standard Views.
--
-- This view is only intended to accessed by a MLab Standard views: breaking changes
-- here will be offset by changes to the Standard views.
--
-- Anything here not visible in a standard view is subject to breaking changes.
--

WITH PreCleanWeb100 AS (
  SELECT
    -- NOTE: we name the partition_date to test_date to prevent exposing
    -- implementation details that are expected to change.
    partition_date AS test_date,
    CONCAT(
      web100_log_entry.connection_spec.local_ip,
      CAST (web100_log_entry.connection_spec.local_port AS STRING),
      web100_log_entry.connection_spec.remote_ip,
      CAST (web100_log_entry.connection_spec.remote_port AS STRING),
      CAST (partition_date AS STRING)
    ) AS psuedoUUID,
    *,
    web100_log_entry.snap.Duration AS connection_duration, -- SYN to FIN total time
    (web100_log_entry.snap.SndLimTimeRwin +
         web100_log_entry.snap.SndLimTimeCwnd +
         web100_log_entry.snap.SndLimTimeSnd) AS measurement_duration, -- Time transfering data
    (blacklist_flags IS NOT NULL and blacklist_flags != 0
        OR anomalies.blacklist_flags IS NOT NULL ) AS b_HasError,
    (web100_log_entry.connection_spec.remote_ip IN
          ("45.56.98.222", "35.192.37.249", "35.225.75.192", "23.228.128.99",
          "2600:3c03::f03c:91ff:fe33:819", "2605:a601:f1ff:fffe::99")
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(web100_log_entry.connection_spec.local_ip),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(web100_log_entry.connection_spec.local_ip),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
        OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(web100_log_entry.connection_spec.local_ip),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
     ) AS b_OAM  -- Data is not from valid clients
  FROM `{{.ProjectID}}.ndt.web100`
  WHERE
    web100_log_entry.snap.Duration IS NOT NULL
    AND web100_log_entry.snap.State IS NOT NULL
    AND web100_log_entry.connection_spec.local_ip IS NOT NULL
    AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
    AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
    AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
    AND web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
),

Web100UploadModels AS (
  SELECT
    test_date,
    -- Struct a models various TCP behaviors
    STRUCT(
      psuedoUUID as UUID,
      log_time AS TestTime,
      "reno" AS CongestionControl,
      web100_log_entry.snap.HCThruOctetsReceived / connection_duration AS MeanThroughputMbps,
      web100_log_entry.snap.MinRTT/1000000 AS MinRTT,  -- Note: sender side
      0 AS LossRate,
      "ndt.web100" AS ToolStack
    ) AS a,
    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      ( -- Upload only, >8kB transfered
        NOT b_OAM AND NOT b_HasError
        AND connection_spec.data_direction IS NOT NULL
        AND connection_spec.data_direction = 1
        AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
        AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
        ) AS ValidBest,
      ( -- Upload only, >kB transfered, 9-60 seconds, > 0 loss
        NOT b_OAM
        AND connection_spec.data_direction IS NOT NULL
        AND connection_spec.data_direction = 1
        AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
        AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000  -- Connection between 9 and 30 seconds (ndt.recommended)
        ) AS Valid2019
    ) AS filter,
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
  FROM PreCleanWeb100
  WHERE
    measurement_duration > 0 AND connection_duration > 0
)

SELECT * FROM Web100UploadModels
