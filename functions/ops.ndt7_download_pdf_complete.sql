-- Create the ops dataset if it does not exist.
CREATE SCHEMA IF NOT EXISTS ops
OPTIONS(location="us");

-- Create or update the table function.
CREATE OR REPLACE TABLE FUNCTION `ops.ndt7_download_pdf_complete`(
    xmin FLOAT64, xmax FLOAT64, field STRING,
    startDate DATE, endDate DATE, siteRegex STRING)
AS (
  SELECT xright, site_pdf, site_cdf, site
  FROM (
    SELECT xright, site_pdf, site_cdf, site,
    FROM `ops.ndt7_download_pdf`(0.1, 3500, field, startDate, endDate, siteRegex)
    UNION ALL
    SELECT xright, site_pdf, site_cdf, site,
    FROM `ops.ndt7_download_pdf_autonode`(0.1, 3500, field, startDate, endDate, siteRegex)
  ) ORDER BY xright, site
);
