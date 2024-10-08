-- Create the ops dataset if it does not exist.
CREATE SCHEMA IF NOT EXISTS ops
OPTIONS(location="us");

-- Create or update the table function.
CREATE OR REPLACE TABLE FUNCTION `ops.ndt7_upload_pdf_autonode`(
    xmin FLOAT64, xmax FLOAT64, field STRING,
    startDate DATE, endDate DATE, siteRegex STRING, mask BOOL)
AS (
  WITH xbins AS (

    SELECT
      POW(10, x-.01) AS xleft,
      POW(10, x+.01) AS xright
    FROM UNNEST(GENERATE_ARRAY(LOG(xmin, 10), LOG(xmax, 10), .02)) AS x

  ), ndt7 AS (

    SELECT *,
      CASE field
        WHEN "MeanThroughputMbps" THEN a.MeanThroughputMbps
        WHEN "MinRTT" THEN a.MinRTT
        ELSE 0
      END AS metric
    FROM `mlab-autojoin.autoload_v2_ndt.ndt7_union`
    WHERE date BETWEEN startDate AND endDate
     AND raw.Upload IS NOT NULL
     AND REGEXP_CONTAINS(server.Site, siteRegex)
     AND IF(mask, NOT a.MeanThroughputMbps BETWEEN 0.42 AND 0.43, TRUE)
     AND ARRAY_LENGTH(raw.Upload.ServerMeasurements) > 0 -- IsComplete
     AND NOT (raw.Upload.ServerMeasurements[SAFE_ORDINAL(ARRAY_LENGTH(raw.Upload.ServerMeasurements))].TCPInfo.BytesReceived < 8192) -- IsSmall
     AND NOT TIMESTAMP_DIFF(raw.Upload.EndTime, raw.Upload.StartTime, MILLISECOND) < 9000 -- IsShort
     AND NOT TIMESTAMP_DIFF(raw.Upload.EndTime, raw.Upload.StartTime, MILLISECOND) > 60000 -- IsLong
     AND server.Site IS NOT NULL

  ), ndt7_cross_xbins AS (

    SELECT
      xright,
      server.Site AS site,
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
