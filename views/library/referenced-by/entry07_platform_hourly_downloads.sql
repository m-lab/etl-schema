#standardSQL

WITH web100_lga03 AS (

  SELECT
    TIMESTAMP_TRUNC(start_time, HOUR) as hour, hostname, COUNT(*) as count
  FROM `{{.ProjectID}}.library.entry07_web100_downloads`
  WHERE
      -- TODO: remove hostname filters.
      hostname LIKE '%lga03%'

  AND mbps is not NULL
  AND mbps > 0
  AND duration > 9

  GROUP BY hostname, hour
  ORDER BY hostname, hour

), result_lga03 AS (

  SELECT
    TIMESTAMP_TRUNC(start_time, hour) as hour,  hostname, count(*) as count
  FROM `{{.ProjectID}}.library.entry07_ndt5_downloads`
  WHERE
      -- TODO: remove hostname filters.
      hostname LIKE '%lga03%'

  -- NOTE: without filtering, we find similar number of raw rows in BQ for both platforms.
  -- When filtering mbps here, we find fewer total tests from the new platform.
  -- Possibly b/c the web100 ndt server still saves parsable data while ndt-server
  -- does not, or due to the ndt-server failing for some legacy clients.
  AND mbps is not NULL
  AND mbps > 0
  AND duration > 9

  GROUP BY hostname, hour
  ORDER BY hostname, hour
)

SELECT * FROM web100_lga03
UNION ALL
SELECT * FROM result_lga03
