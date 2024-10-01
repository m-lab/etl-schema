-- DEPRECATED: this name is preserved for legacy applications. Use
--   ops.ndt7_download_pdf_managed instead.

-- Create the ops dataset if it does not exist.
CREATE SCHEMA IF NOT EXISTS ops
OPTIONS(location="us");

-- Create or update the table function.
CREATE OR REPLACE TABLE FUNCTION `ops.ndt7_download_pdf`(
    xmin FLOAT64, xmax FLOAT64, field STRING,
    startDate DATE, endDate DATE, siteRegex STRING)
AS (
  SELECT * FROM `ops.ndt7_download_pdf_managed`(xmin, xmax, field, startDate, endDate, siteRegex)
);
