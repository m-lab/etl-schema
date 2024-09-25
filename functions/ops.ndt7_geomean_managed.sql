-- Create the ops dataset if it does not exist.
CREATE SCHEMA IF NOT EXISTS ops
OPTIONS(location="us");

-- Create or update the table function.
CREATE OR REPLACE TABLE FUNCTION `ops.ndt7_geomean_managed`(
    field STRING, startDate DATE, endDate DATE, siteRegex STRING)
AS (
  SELECT
    server.Site,
    COUNT(*) AS tests,
    AVG(
      CASE field
        WHEN "MeanThroughputMbps" THEN a.MeanThroughputMbps
        WHEN "MinRTT" THEN a.MinRTT
        WHEN "LossRate" THEN a.LossRate
        ELSE 0
      END) AS mean,
    EXP(AVG(LN(
      CASE field
        WHEN "MeanThroughputMbps" THEN a.MeanThroughputMbps
        WHEN "MinRTT" THEN a.MinRTT
        WHEN "LossRate" THEN a.LossRate
        ELSE 0
      END))) AS geoMean,
  FROM `measurement-lab.ndt_intermediate.extended_ndt7_downloads`
  WHERE date BETWEEN startDate AND endDate
    AND REGEXP_CONTAINS(server.Site, siteRegex)
    AND (filter.IsComplete AND filter.IsProduction AND NOT filter.IsError AND
          NOT filter.IsOAM AND NOT filter.IsPlatformAnomaly AND NOT filter.IsSmall AND
          (filter.IsEarlyExit OR NOT filter.IsShort) AND NOT filter.IsLong AND NOT filter._IsRFC1918)
    AND server.Site IS NOT NULL
  GROUP BY server.Site
)
