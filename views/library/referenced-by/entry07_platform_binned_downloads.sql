#standardSQL
/*

  Histogram bins for NDT downloads, rounded to one decimal place on a log scale.

*/

WITH web100_lga03 AS (

  SELECT ROUND(EXP(ROUND(LOG(mbps),1)), 1) AS bin, hostname, COUNT(*) as count
  FROM   `{{.ProjectID}}.library.entry07_web100_downloads`
  WHERE
    -- TODO: remove host filters.
    hostname LIKE '%lga03%'
  AND mbps > 0 AND mbps < 10000
  AND duration BETWEEN 9 and 12
  -- NOTE: attempt to inlcude only onebox tests. Some errors may be missed.
  AND protocol IN("truetrue")

  GROUP BY bin, hostname
  ORDER BY bin

), ndt5_lga03 AS (

  SELECT ROUND(EXP(ROUND(LOG(mbps),1)), 1) AS bin, hostname, COUNT(*) as count
  FROM   `{{.ProjectID}}.library.entry07_ndt5_downloads`
  WHERE
    -- TODO: remove host filters.
    hostname LIKE '%lga03%'
  -- NOTE: include only "onebox" tests.
  AND protocol IN("WSSJSON")
  AND mbps > 0 AND mbps < 10000

  GROUP BY bin, hostname
  ORDER BY bin
)

SELECT FORMAT("%08.1f", bin) as bin, hostname, count FROM ndt5_lga03
UNION ALL
SELECT FORMAT("%08.1f", bin) as bin, hostname, count FROM web100_lga03
ORDER BY bin
