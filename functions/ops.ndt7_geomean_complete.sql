-- Create the ops dataset if it does not exist.
CREATE SCHEMA IF NOT EXISTS ops
OPTIONS(location="us");

-- Create or update the table function.
CREATE OR REPLACE TABLE FUNCTION `ops.ndt7_geomean_complete`(
    field STRING, startDate DATE, endDate DATE, siteRegex STRING)
AS (
  SELECT field, *
  FROM `ops.ndt7_geomean_managed`(field, startDate, endDate, siteRegex)
  UNION ALL
  SELECT field, *
  FROM `ops.ndt7_geomean_autonode`(field, startDate, endDate, siteRegex)
)

