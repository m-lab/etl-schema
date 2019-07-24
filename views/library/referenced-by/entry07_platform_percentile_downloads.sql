/*

  Percentile data for Q-Q style plots, comparing legacy web100 and k8s ndt5
  download data by date and percentile.

*/

WITH legacy AS (
  SELECT
    DATE(start_time) AS date,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(ROUND(mbps,3), 100) AS percentiles
  FROM `{{.ProjectID}}.library.entry07_web100_downloads`
  WHERE
    -- TODO: remove site filter.
    hostname LIKE "%mlab2.lga03%"
  GROUP BY
    date
),

legacy_quantiles AS (
  SELECT date, downloads, value, index FROM legacy, legacy.percentiles AS value WITH OFFSET AS index
),

ndt5 AS (
  SELECT
    DATE(start_time) AS date,
    COUNT(*) AS downloads,
    APPROX_QUANTILES(ROUND(mbps,3), 100) AS percentiles
  FROM `{{.ProjectID}}.library.entry07_ndt5_downloads`
  WHERE
    -- TODO: remove site filter.
    hostname LIKE "%lga03%"
    AND duration > 9
  GROUP BY
    date
),

ndt5_quantiles AS (
    SELECT date, downloads, value, index FROM ndt5, ndt5.percentiles AS value WITH OFFSET AS index
),

all_dates AS (
  SELECT
    ndt5_quantiles.date,
    ndt5_quantiles.downloads AS ndt5_downloads,
    legacy_quantiles.downloads AS legacy_downloads,
    ndt5_quantiles.index AS percentile,
    ndt5_quantiles.value AS ndt5,
    legacy_quantiles.value AS legacy
  FROM
    ndt5_quantiles JOIN legacy_quantiles ON (
          ndt5_quantiles.index=legacy_quantiles.index
      AND ndt5_quantiles.date=legacy_quantiles.date)
  ORDER BY
    date, percentile
)

SELECT
  -- NOTE: cast as a string to easily use as a datastudio "dimension".
  CAST(date AS string) as date,
  ndt5_downloads,
  legacy_downloads,
  percentile,
  ndt5,
  legacy

FROM
  all_dates

ORDER BY
  date, percentile
