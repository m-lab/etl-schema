#standardSQL
/*

  This query extracts web100 download tests, filtering monitoring IPs.

  The server hostname is reported as a canonical, abbreviated hostname e.g.
  "mlab1.lga03".

*/

SELECT
    log_time as start_time,
    8 * (web100_log_entry.snap.HCThruOctetsAcked /
        (web100_log_entry.snap.SndLimTimeRwin +
         web100_log_entry.snap.SndLimTimeCwnd +
         web100_log_entry.snap.SndLimTimeSnd)) AS mbps,
    REPLACE(
      connection_spec.server_hostname, ".measurement-lab.org", "") as hostname,
    CONCAT(
      cast(connection_spec.websockets as string),
      cast(connection_spec.tls as string)) AS protocol,
    (web100_log_entry.snap.SndLimTimeRwin +
      web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd) / 1000000.0 as duration

FROM `{{.ProjectID}}.ndt.web100`

WHERE
      partition_date BETWEEN DATE("2019-07-19") AND DATE("2019-07-29")
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip NOT IN(
    "45.56.98.222", "35.192.37.249", "35.225.75.192",
    "2600:3c03::f03c:91ff:fe33:819", "23.228.128.99", "2605:a601:f1ff:fffe::99")
  -- Download direction, and at least 8KB transfered
  AND connection_spec.data_direction IS NOT NULL
  AND connection_spec.data_direction = 1
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
  -- Sum of SndLimTime* have real values.
  AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
  -- NOTE: this filter does not exclude tests with CongSignals > 0 because we
  -- want to compare aggreate test counts.
