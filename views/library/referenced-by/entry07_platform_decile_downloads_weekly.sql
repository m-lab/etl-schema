/*

  Decile data for Q-Q style plots, comparing web100 web100 and k8s ndt5
  download data by hostname over entire timeperiod of low-level views.

*/



WITH web100_dedup AS (
  SELECT
    hostname,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(ROUND(mbps,3), 10) AS deciles
  FROM `{{.ProjectID}}.library.entry07_web100_downloads`
  WHERE REGEXP_CONTAINS(hostname, "mlab[23]")
  GROUP BY hostname
), web100 AS (
  SELECT
    hostname,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(ROUND(mbps,3), 10) AS deciles
  FROM `{{.ProjectID}}.library.entry07_web100_downloads`
  WHERE REGEXP_CONTAINS(hostname, "mlab[23]")
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
    APPROX_QUANTILES(ROUND(mbps,3), 10) AS deciles
  FROM `{{.ProjectID}}.library.entry07_ndt5_downloads`
  WHERE duration > 9
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
