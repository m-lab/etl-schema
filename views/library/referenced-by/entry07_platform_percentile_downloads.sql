/*

  Percentile data for Q-Q style plots, comparing legacy web100 and k8s ndt5
  download data by date and percentile.

*/

WITH legacy AS (
  SELECT
    DATE(start_time) AS date,
    COUNT(*) as downloads,
    APPROX_QUANTILES(ROUND(mbps,3), 100) AS percentiles
  FROM `{{.ProjectID}}.library.entry07_web100_downloads`
  WHERE
    -- TODO: remove site filter.
    hostname LIKE "%mlab2.lga03%"
  GROUP BY
    date
),

legacy_quantiles AS (
  SELECT date, downloads, bin, index FROM legacy, legacy.percentiles AS bin WITH OFFSET AS index
),

ndt5 AS (
  SELECT
    DATE(start_time) as date,
    COUNT(*) as downloads,
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
    SELECT date, downloads, bin, index FROM ndt5, ndt5.percentiles AS bin WITH OFFSET AS index
),

all_dates AS (
  SELECT
    ndt5_quantiles.date,
    ndt5_quantiles.downloads as ndt5_downloads,
    legacy_quantiles.downloads as legacy_downloads,
    ndt5_quantiles.index AS percentile,
    ndt5_quantiles.bin AS ndt5,
    legacy_quantiles.bin AS legacy
  FROM
    ndt5_quantiles JOIN legacy_quantiles ON (
          ndt5_quantiles.index=legacy_quantiles.index
      AND ndt5_quantiles.date=legacy_quantiles.date)
  ORDER BY
    date, percentile
)

SELECT
  -- NOTE: cast as a string to easily use as a datastudio "dimension".
  CAST(date as string) as date,
  ndt5_downloads,
  legacy_downloads,
  percentile,
  ndt5,
  legacy

FROM
  all_dates

ORDER BY
  date, percentile
