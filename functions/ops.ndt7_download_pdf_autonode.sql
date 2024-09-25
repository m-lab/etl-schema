-- Create the ops dataset if it does not exist.
CREATE SCHEMA IF NOT EXISTS ops
OPTIONS(location="us");

-- Create or update the table function.
CREATE OR REPLACE TABLE FUNCTION `ops.ndt7_download_pdf_autonode`(
    xmin FLOAT64, xmax FLOAT64, field STRING,
    startDate DATE, endDate DATE, siteRegex STRING)
AS (
  WITH xbins AS (

    SELECT
      POW(10, x-.01) AS xleft,
      POW(10, x+.01) AS xright
    FROM UNNEST(GENERATE_ARRAY(LOG(xmin, 10), LOG(xmax, 10), .02)) AS x

  ), ndt7 AS (

    SELECT server.Site as site,
      CASE field
        WHEN "MeanThroughputMbps" THEN a.MeanThroughputMbps
        WHEN "MinRTT" THEN a.MinRTT
        WHEN "LossRate" THEN a.LossRate
        ELSE 0
      END AS metric
    FROM `mlab-autojoin.autoload_v2_ndt.ndt7_union`
    WHERE date BETWEEN startDate AND endDate
     AND REGEXP_CONTAINS(server.Site, siteRegex)
     AND raw.Download IS NOT NULL
     AND ARRAY_LENGTH(raw.Download.ServerMeasurements) > 0 -- IsComplete
     AND NOT (raw.Download.ServerMeasurements[SAFE_ORDINAL(ARRAY_LENGTH(raw.Download.ServerMeasurements))].TCPInfo.BytesAcked < 8192) -- IsSmall
     AND (
      IF("early_exit" IN (SELECT metadata.Name FROM UNNEST(raw.Download.ClientMetadata) AS metadata), True, False) OR
      NOT TIMESTAMP_DIFF(raw.Download.EndTime, raw.Download.StartTime, MILLISECOND) < 9000 -- IsShort
     )
     AND NOT TIMESTAMP_DIFF(raw.Download.EndTime, raw.Download.StartTime, MILLISECOND) > 60000 -- IsLong
     AND server.Site IS NOT NULL

  ), ndt7_cross_xbins AS (

    SELECT
      xright,
      site,
      IF(metric BETWEEN xleft AND xright, 1, 0) AS present,
    FROM ndt7 CROSS JOIN xbins
    WHERE metric BETWEEN xmin AND xmax

  ), ndt7_xbins_counts AS (

    SELECT
      xright,
      site,
      SUM(present) AS bin_count,
    FROM   ndt7_cross_xbins
    GROUP BY xright, site
    ORDER BY xright

  ), ndt7_xbins_counts_site_pdf AS (

    SELECT
      xright,
      site,
      bin_count,
      -- Divide bin count by total number of samples for each site, to normalize counts for all sites.
      bin_count / SUM(bin_count) OVER (partition by site) AS site_pdf,
    FROM ndt7_xbins_counts
    ORDER BY xright

  )

  SELECT
    xright,
    site,
    site_pdf,
    SUM(site_pdf) OVER (PARTITION BY site ORDER BY xright ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS site_cdf,
  FROM ndt7_xbins_counts_site_pdf
);
