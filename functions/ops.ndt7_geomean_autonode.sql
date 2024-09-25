-- Create the ops dataset if it does not exist.
CREATE SCHEMA IF NOT EXISTS ops
OPTIONS(location="us");

-- Create or update the table function.
CREATE OR REPLACE TABLE FUNCTION `ops.ndt7_geomean_autonode`(
    field STRING, startDate DATE, endDate DATE, siteRegex STRING)
AS (
  SELECT
    server.Site as site,
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
  GROUP BY server.Site
)
