-- Create the ops dataset if it does not exist.
CREATE SCHEMA IF NOT EXISTS ops
OPTIONS(location="us");

-- Create or update the table function.
CREATE OR REPLACE TABLE FUNCTION `ops.ndt7_geomean_autonode`(
    field STRING, startDate DATE, endDate DATE, siteRegex STRING)
AS (
  SELECT
    server.Site,
    COUNT(*) AS tests,
    AVG(a.MeanThroughputMbps) as meanRate,
    EXP(AVG(LN(a.MeanThroughputMbps))) AS GeometricMeanRate,
  FROM `measurement-lab.ndt_intermediate.extended_ndt7_downloads`
  WHERE date BETWEEN startDate AND endDate
    AND REGEXP_CONTAINS(server.Site, siteRegex)
    AND (filter.IsComplete AND filter.IsProduction AND NOT filter.IsError AND
          NOT filter.IsOAM AND NOT filter.IsPlatformAnomaly AND NOT filter.IsSmall AND
          (filter.IsEarlyExit OR NOT filter.IsShort) AND NOT filter.IsLong AND NOT filter._IsRFC1918)
    AND server.Site IS NOT NULL
  GROUP BY server.Site
)
