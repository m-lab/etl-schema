/*

  Decile data for Q-Q style plots, comparing web100 web100 and k8s ndt5
  download data by date and decile.

*/

WITH web100 AS (
  SELECT
    DATE(start_time) AS date,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(ROUND(mbps,3), 10) AS deciles
  FROM `{{.ProjectID}}.library.entry07_web100_downloads`
  WHERE
    -- TODO: remove site filter.
    hostname LIKE "%mlab2.lga03%"
    AND CAST(protocol AS STRING) IN("null", "truetrue")
    AND mbps > 0.1
  GROUP BY
    date
),

web100_quantiles AS (
  SELECT date, downloads, value, index FROM web100, web100.deciles AS value WITH OFFSET AS index
),

ndt5 AS (
  SELECT
    DATE(start_time) AS date,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(ROUND(mbps,3), 10) AS deciles
  FROM `{{.ProjectID}}.library.entry07_ndt5_downloads`
  WHERE
    -- TODO: remove site filter.
    hostname LIKE "%lga03%"
    AND CAST(protocol AS STRING) IN("null", "WSS+JSON")
    AND mbps > 0
    AND duration > 9
  GROUP BY
    date
),

ndt5_quantiles AS (
    SELECT date, downloads, value, index FROM ndt5, ndt5.deciles AS value WITH OFFSET AS index
),

all_dates AS (
  SELECT
    ndt5_quantiles.date,
    ndt5_quantiles.downloads AS ndt5_downloads,
    web100_quantiles.downloads AS web100_downloads,
    ndt5_quantiles.index AS decile,
    ndt5_quantiles.value AS ndt5,
    web100_quantiles.value AS web100
  FROM
    ndt5_quantiles JOIN web100_quantiles ON (
          ndt5_quantiles.index=web100_quantiles.index
      AND ndt5_quantiles.date=web100_quantiles.date)
  ORDER BY
    date, decile
)

SELECT
  -- NOTE: cast as a string to easily use as a datastudio "dimension".
  CAST(date AS string) as date,
  ndt5_downloads,
  web100_downloads,
  decile,
  ndt5,
  web100

FROM
  all_dates

ORDER BY
  date, decile
