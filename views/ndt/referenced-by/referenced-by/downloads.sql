#standardSQL
SELECT
      -- All good quality download tests
       *,
      -- A synthetic column that conforms to our best practices.
      -- The times are in microseconds, and dividing total throughput by total
      -- microseconds yields mean throughput in Megabits per second.  This
      -- query excludes any time during which the connection was quiescent,
      -- and as such is a fairer representation of how fast we were able to
      -- push data down the pipe as compared to just dividing by
      -- web100_log_entry.snap.Duration
      --
      -- TODO: Delete this once https://github.com/m-lab/etl/issues/663 is
      -- resolved.
      STRUCT(
        8 * (web100_log_entry.snap.HCThruOctetsAcked /
        (web100_log_entry.snap.SndLimTimeRwin +
         web100_log_entry.snap.SndLimTimeCwnd +
         web100_log_entry.snap.SndLimTimeSnd)) AS mean_download_throughput_mbps
      ) as alpha
FROM `{{.ProjectID}}.ndt.recommended`
WHERE
  -- download direction, and at least 8KB transfered
  connection_spec.data_direction IS NOT NULL
  AND connection_spec.data_direction = 1
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
  -- sum of SndLimTime is sensible - more than 9 seconds, less than 1 minute
  AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd BETWEEN 9000000 AND 60000000
  -- Congestion was detected
  -- Note that this removes a large portion of download tests!!!
  AND web100_log_entry.snap.CongSignals IS NOT NULL AND web100_log_entry.snap.CongSignals > 0
