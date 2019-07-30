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
    WHERE CAST(protocol AS STRING) IN("null", "truetrue")
), raw_web100_max AS (
    SELECT *
    FROM raw_web100_remote
    WHERE row_number = 1
), raw_ndt5 AS (
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
   SELECT *, ROW_NUMBER() OVER(Partition BY remote_ip ORDER BY mbps DESC) AS row_number
   FROM raw_ndt5
   WHERE CAST(protocol AS STRING) IN("null", "WSS+JSON")
), raw_ndt5_max AS (
  SELECT *
  FROM raw_ndt5_remote
  WHERE row_number = 1
), web100 AS (
  SELECT
    DATE(start_time) AS date,
    hostname,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(TRUNC(mbps * duration / 3.2) * 3.2/duration, 10) AS deciles -- truncate to integer # of onebox msgs.
  FROM raw_web100_max
  WHERE
        mbps IS NOT NULL
    AND TRUNC(mbps * duration / 3.2) >= 1 -- require at least one message received.
    AND duration > 9
    AND REGEXP_CONTAINS(hostname, "mlab[23]")
  GROUP BY
    date, hostname
),

web100_quantiles AS (
  SELECT date, REGEXP_EXTRACT(hostname, "mlab[23].(.*)") as site, hostname, downloads, value, index FROM web100, web100.deciles AS value WITH OFFSET AS index
),

ndt5 AS (
  SELECT
    DATE(start_time) AS date,
    hostname,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(mbps, 10) AS deciles
  FROM raw_ndt5_max
  WHERE
        mbps IS NOT NULL
    AND mbps > 0 -- require at least one message received.
    AND duration > 9
    AND REGEXP_CONTAINS(hostname, "mlab[23]")
  GROUP BY
    date, hostname
),

ndt5_quantiles AS (
    SELECT date, REGEXP_EXTRACT(hostname, "mlab[23].(.*)") as site, hostname, downloads, value, index FROM ndt5, ndt5.deciles AS value WITH OFFSET AS index
),

all_hostnames AS (
  SELECT
    ndt5_quantiles.date,
    ndt5_quantiles.site,
    ndt5_quantiles.downloads AS ndt5_downloads,
    web100_quantiles.downloads AS web100_downloads,
    ndt5_quantiles.index AS decile,
    ndt5_quantiles.value AS ndt5,
    web100_quantiles.value AS web100
  FROM
    ndt5_quantiles JOIN web100_quantiles ON (
          ndt5_quantiles.index=web100_quantiles.index
          AND ndt5_quantiles.date=web100_quantiles.date
          AND ndt5_quantiles.site=web100_quantiles.site)
  ORDER BY
    site, decile
)


SELECT
  -- NOTE: cast as a string to easily use as a datastudio "dimension".
  CAST(date AS string) as date,
  site,
  ndt5_downloads,
  web100_downloads,
  decile,
  ndt5,
  web100

FROM
  all_hostnames

ORDER BY
  date, site, decile
