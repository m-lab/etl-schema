/*
  legacy vs ndt5 by date, based on summary w/ tcpinfo, lga03, duration > 9
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

legacyQuantiles AS (
  SELECT date, downloads, d, o FROM legacy, legacy.percentiles AS d WITH OFFSET AS o
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

ndt5Quantiles AS (
    SELECT date, downloads, d, o FROM ndt5, ndt5.percentiles AS d WITH OFFSET AS o
),

allDates AS (
  SELECT
    ndt5Quantiles.date,
    ndt5Quantiles.downloads as ndt5Downloads,
    legacyQuantiles.downloads as legacyDownloads,
    ndt5Quantiles.o AS percentile,
    ndt5Quantiles.d AS ndt5,
    legacyQuantiles.d AS legacy
  FROM
    ndt5Quantiles JOIN legacyQuantiles ON (
          ndt5Quantiles.o=legacyQuantiles.o
      AND ndt5Quantiles.date=legacyQuantiles.date)
  ORDER BY
    date, percentile
)

SELECT
  CAST(date as string) as date,
  ndt5Downloads,
  legacyDownloads,
  percentile,
  ndt5,
  legacy

FROM
  allDates

ORDER BY
  date, percentile
