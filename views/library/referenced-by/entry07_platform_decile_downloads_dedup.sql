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
      partition_date BETWEEN DATE("2019-07-19") AND DATE("2019-07-25")
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
  AND REGEXP_CONTAINS(connection_spec.server_hostname, "mlab[23].(lax02|lga03|ams03|bom02)")
  -- NOTE: this filter does not exclude tests with CongSignals > 0 because we
  -- want to compare aggreate test counts.
), raw_web100_remote AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY remote_ip ORDER BY mbps DESC) AS row_number
    FROM raw_web100
    WHERE CAST(protocol AS STRING) IN("null", "truetrue")
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
      DATE(result.StartTime) BETWEEN DATE("2019-07-19") AND DATE("2019-07-25")
  AND result.S2C IS NOT NULL
  AND result.S2C.ClientIP IS NOT NULL
  AND result.S2C.ClientIP NOT IN(
    "45.56.98.222", "35.192.37.249", "35.225.75.192",
    "2600:3c03::f03c:91ff:fe33:819", "23.228.128.99", "2605:a601:f1ff:fffe::99")

), raw_ndt5_remote AS (
   select *, ROW_NUMBER() OVER(Partition BY remote_ip ORDER BY mbps DESC) AS row_number FROM raw_ndt5 WHERE CAST(protocol AS STRING) IN("null", "WSS+JSON")
), raw_ndt5_max AS (select * from raw_ndt5_remote where row_number = 1

), web100 AS (
  SELECT
    hostname,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(ROUND(mbps,3), 10) AS deciles
  FROM `raw_web100_max`
  WHERE
         mbps IS NOT NULL
     AND mbps > 0.05
     AND duration > 9
     AND REGEXP_CONTAINS(hostname, "mlab[23]")
  GROUP BY
    hostname
),

web100_quantiles AS (
  SELECT REGEXP_EXTRACT(hostname, "mlab[23].(.*)") as site, hostname, downloads, value, index FROM web100, web100.deciles AS value WITH OFFSET AS index
),

ndt5 AS (
  SELECT
    hostname,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(ROUND(mbps ,3), 10) AS deciles
  FROM raw_ndt5_max
  WHERE
          mbps IS NOT NULL
      AND mbps > 0
      AND duration > 9
      AND REGEXP_CONTAINS(hostname, "mlab[23]")
  GROUP BY
    hostname
),

ndt5_quantiles AS (
    SELECT REGEXP_EXTRACT(hostname, "mlab[23].(.*)") as site, hostname, downloads, value, index FROM ndt5, ndt5.deciles AS value WITH OFFSET AS index
),

all_hostnames AS (
  SELECT
    ndt5_quantiles.site,
    ndt5_quantiles.downloads AS ndt5_downloads,
    web100_quantiles.downloads AS web100_downloads,
    ndt5_quantiles.index AS decile,
    ndt5_quantiles.value AS ndt5,
    web100_quantiles.value AS web100
  FROM
    ndt5_quantiles JOIN web100_quantiles ON (
          ndt5_quantiles.index=web100_quantiles.index
          AND ndt5_quantiles.site=web100_quantiles.site)
  ORDER BY
    site, decile
)


SELECT
  -- NOTE: cast as a string to easily use as a datastudio "dimension".
  site,
  ndt5_downloads,
  web100_downloads,
  decile,
  ndt5,
  web100

FROM
  all_hostnames

ORDER BY
  site, decile
