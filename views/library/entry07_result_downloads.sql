#standardSQL
/*

  Download summary from ndt-server ndt5 protocol result data,
  filtering monitoring IPs.

  The server hostname is reported as a canonical, abbreviated hostname e.g.
  "mlab3.lga03".
*/
SELECT
    result.StartTime as start_time,
    result.S2C.MeanThroughputMbps as mbps,
    CONCAT(result.Control.Protocol, result.Control.MessageProtocol) as protocol,
    REPLACE(REGEXP_EXTRACT(ParseInfo.TaskFileName, "-(mlab[1-4]-[a-z]{3}[0-9]{2})-"), "-", ".") AS hostname,
    TIMESTAMP_DIFF(result.S2C.EndTime, result.S2C.StartTime, MILLISECOND)/1000 as duration

FROM `{{.ProjectID}}.base_tables.result`

WHERE
      DATE(result.StartTime) BETWEEN DATE("2019-07-19") AND DATE("2019-07-29")
  AND result.S2C IS NOT NULL
  AND result.S2C.ClientIP IS NOT NULL
  AND result.S2C.ClientIP NOT IN(
    "45.56.98.222", "35.192.37.249", "35.225.75.192",
    "2600:3c03::f03c:91ff:fe33:819", "23.228.128.99", "2605:a601:f1ff:fffe::99")
