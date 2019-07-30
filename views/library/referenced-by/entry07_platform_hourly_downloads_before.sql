#standardSQL

/*
WITH web100_lga03 AS (

  SELECT
    TIMESTAMP_TRUNC(start_time, HOUR) as hour, hostname, COUNT(*) as count
  FROM `{{.ProjectID}}.library.entry07_web100_downloads`
  WHERE
      mbps is not NULL
  AND mbps > 0.1
  AND duration > 9

  GROUP BY hostname, hour
  ORDER BY hostname, hour

), result_lga03 AS (

  SELECT
    TIMESTAMP_TRUNC(start_time, hour) as hour,  hostname, count(*) as count
  FROM `{{.ProjectID}}.library.entry07_ndt5_downloads`
  WHERE
  -- NOTE: without filtering, we find similar number of raw rows in BQ for both platforms.
  -- When filtering mbps here, we find fewer total tests from the new platform.
  -- Possibly b/c the web100 ndt server still saves parsable data while ndt-server
  -- does not, or due to the ndt-server failing for some legacy clients.
      mbps is not NULL
  AND mbps > 0
  AND duration > 9

  GROUP BY hostname, hour
  ORDER BY hostname, hour
)
*/
WITH raw_web100 as (
  SELECT
    log_time as start_time,
    web100_log_entry.connection_spec.remote_ip as remote_ip,
    8 * (web100_log_entry.snap.HCThruOctetsAcked /
        (web100_log_entry.snap.SndLimTimeRwin +
         web100_log_entry.snap.SndLimTimeCwnd +
         web100_log_entry.snap.SndLimTimeSnd)) AS mbps,
    REPLACE(
      connection_spec.server_hostname, ".measurement-lab.org", "") as hostname,
    CONCAT(
      cast(connection_spec.websockets as string),
      cast(connection_spec.tls as string)) AS protocol,
    (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd) / 1000000.0 as duration

FROM `mlab-oti.ndt.web100`

WHERE
      partition_date BETWEEN DATE("2019-07-10") AND DATE("2019-07-16")
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip NOT IN(
    "45.56.98.222", "35.192.37.249", "35.225.75.192",
    "2600:3c03::f03c:91ff:fe33:819", "23.228.128.99", "2605:a601:f1ff:fffe::99")
  -- Download direction, and at least 8KB transfered
  AND connection_spec.data_direction IS NOT NULL
  AND connection_spec.data_direction = 1
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
  -- Sum of SndLimTime* have real values.
  AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
  AND REGEXP_CONTAINS(connection_spec.server_hostname, "mlab[123].(lax02|lga03|ams03|bom02)")
  -- NOTE: this filter does not exclude tests with CongSignals > 0 because we
  -- want to compare aggreate test counts.
), raw_web100_remote AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY remote_ip ORDER BY mbps DESC) AS row_number
    FROM raw_web100
), raw_web100_max AS (
    SELECT *
    FROM raw_web100_remote
    WHERE row_number = 1
), raw_ndt5 AS (
  SELECT
    result.StartTime as start_time,
    result.S2C.MeanThroughputMbps as mbps,
    result.ClientIP AS remote_ip,
    CONCAT(result.Control.Protocol, "+", result.Control.MessageProtocol) as protocol,
    REPLACE(REGEXP_EXTRACT(ParseInfo.TaskFileName, "-(mlab[1-4]-[a-z]{3}[0-9]{2})-"), "-", ".") AS hostname,
    TIMESTAMP_DIFF(result.S2C.EndTime, result.S2C.StartTime, MILLISECOND)/1000 as duration

  -- TODO: use 'ndt5' as table name.
  FROM `mlab-oti.base_tables.result`

  WHERE
      DATE(result.StartTime) BETWEEN DATE("2019-07-10") AND DATE("2019-07-16")
  AND result.S2C IS NOT NULL
  AND result.S2C.ClientIP IS NOT NULL
  AND result.S2C.ClientIP NOT IN(
    "45.56.98.222", "35.192.37.249", "35.225.75.192",
    "2600:3c03::f03c:91ff:fe33:819", "23.228.128.99", "2605:a601:f1ff:fffe::99")

), raw_ndt5_remote AS (
   select *, ROW_NUMBER() OVER(Partition BY remote_ip ORDER BY mbps DESC) AS row_number
   FROM raw_ndt5
), raw_ndt5_max AS (
  SELECT *
  FROM raw_ndt5_remote
  WHERE row_number = 1
), web100_lga03 AS (
  SELECT
    TIMESTAMP_TRUNC(start_time, hour) as hour,
    hostname,
    COUNT(*) AS count
  FROM raw_web100_max
  WHERE
        mbps is not NULL
    -- AND mbps > 0.05
    AND TRUNC(mbps * duration / 3.2) >= 1
    AND duration > 9
    AND REGEXP_CONTAINS(hostname, "mlab[123]")
  GROUP BY
    hour, hostname
),

ndt5_lga03 AS (
  SELECT
    TIMESTAMP_TRUNC(start_time, hour) as hour,
    hostname,
    COUNT(*) AS count
  FROM raw_ndt5_max
  WHERE
        mbps is not NULL
    AND mbps > 0
    AND duration > 9
    AND REGEXP_CONTAINS(hostname, "mlab[123]")
  GROUP BY
    hour, hostname
)


select * from(
SELECT * FROM web100_lga03
UNION ALL
SELECT * FROM ndt5_lga03
)
order by hour, hostname
