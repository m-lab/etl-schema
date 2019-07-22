#standardSQL
-- All NDT tests except:
--   internal test from EB.
--   blacklisted tests
-- Where
--  TCP end state is sensible
--  Test duration was between 9 and 60 seconds.
SELECT * 
FROM `{{.ProjectID}}.ndt.web100`
WHERE
  -- not blacklisted
  (blacklist_flags = 0 OR
    (blacklist_flags IS NULL AND anomalies.blacklist_flags IS NULL))
  -- not from EB monitoring or unknown client
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip NOT IN("45.56.98.222", "35.192.37.249", "35.225.75.192", "2600:3c03::f03c:91ff:fe33:819", "23.228.128.99", "2605:a601:f1ff:fffe::99")
  -- sensible TCP end state
  AND web100_log_entry.snap.State IS NOT NULL
  AND web100_log_entry.snap.State IN (1,5,6,7,8,9,10,11)
  -- sensible test duration
  AND web100_log_entry.snap.Duration IS NOT NULL
  AND web100_log_entry.snap.Duration BETWEEN 9000000 AND 60000000  -- between 9 seconds and 1 minute
