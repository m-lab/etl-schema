#standardSQL

WITH raw_web100 as (
  SELECT
    log_time as start_time,
    web100_log_entry.connection_spec.remote_ip as remote_ip,
    8 * SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsReceived, web100_log_entry.snap.Duration) AS mbps,
    REPLACE(connection_spec.server_hostname, ".measurement-lab.org", "") as hostname,
    CONCAT(
      cast(connection_spec.websockets as string),
      cast(connection_spec.tls as string)) AS protocol,
    (web100_log_entry.snap.Duration) / 1000000.0 as duration

  FROM `mlab-oti.ndt.web100`

  WHERE
      partition_date BETWEEN DATE("2019-07-19") AND DATE("2019-07-25")
  -- not from EB monitoring or unknown client
    AND web100_log_entry.connection_spec.local_ip IS NOT NULL
    AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
    AND web100_log_entry.connection_spec.remote_ip NOT IN(
      "45.56.98.222", "35.192.37.249", "35.225.75.192",
      "2600:3c03::f03c:91ff:fe33:819", "23.228.128.99", "2605:a601:f1ff:fffe::99")
    -- sensible test duration
    AND web100_log_entry.snap.Duration IS NOT NULL
    AND connection_spec.data_direction IS NOT NULL
    AND connection_spec.data_direction = 0
    -- sensible total bytes received.
    AND web100_log_entry.snap.HCThruOctetsReceived IS NOT NULL
    AND web100_log_entry.snap.HCThruOctetsReceived >= 8192
    AND REGEXP_CONTAINS(connection_spec.server_hostname, "mlab[123].(lax02|lga03|ams03|bom02)")
), raw_web100_remote AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY remote_ip ORDER BY mbps DESC) AS row_number
    FROM raw_web100
), raw_web100_max AS (
    SELECT *
    FROM raw_web100_remote
    WHERE row_number = 1
),

raw_ndt5 AS (
  SELECT
    result.StartTime as start_time,
    result.C2S.MeanThroughputMbps as mbps,
    result.ClientIP AS remote_ip,
    CONCAT(result.Control.Protocol, "+", result.Control.MessageProtocol) as protocol,
    REPLACE(REGEXP_EXTRACT(ParseInfo.TaskFileName, "-(mlab[1-4]-[a-z]{3}[0-9]{2})-"), "-", ".") AS hostname,
    TIMESTAMP_DIFF(result.C2S.EndTime, result.C2S.StartTime, MILLISECOND)/1000 as duration

  FROM `mlab-oti.base_tables.result`

  WHERE
      DATE(result.StartTime) BETWEEN DATE("2019-07-19") AND DATE("2019-07-25")
    AND result.C2S IS NOT NULL
    AND result.C2S.ClientIP IS NOT NULL
    AND result.C2S.ClientIP NOT IN(
      "45.56.98.222", "35.192.37.249", "35.225.75.192",
      "2600:3c03::f03c:91ff:fe33:819", "23.228.128.99", "2605:a601:f1ff:fffe::99")

), raw_ndt5_remote AS (
   select *, ROW_NUMBER() OVER(Partition BY remote_ip ORDER BY mbps DESC) AS row_number
   FROM raw_ndt5
), raw_ndt5_max AS (
  SELECT *
  FROM raw_ndt5_remote
  WHERE row_number = 1
),

web100_hosts AS (
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

ndt5_hosts AS (
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
SELECT * FROM web100_hosts
UNION ALL
SELECT * FROM ndt5_hosts
)
order by hour, hostname
