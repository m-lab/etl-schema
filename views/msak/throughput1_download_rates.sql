WITH
time_range AS (
  SELECT raw.MeasurementID as mid, min(raw.StartTime) as start_time, min(raw.EndTime) as end_time
  FROM `{{.ProjectID}}.msak.throughput1`
  WHERE raw.Direction = "download"
  GROUP BY raw.MeasurementID
),

actual_elapsed_times AS (
  SELECT raw.MeasurementID as mid, max(sm.ElapsedTime) as elapsed
  FROM `{{.ProjectID}}.msak.throughput1` msak
    JOIN UNNEST(raw.ServerMeasurements) sm
    JOIN time_range r ON msak.raw.MeasurementID = r.mid

  -- Verify that the test's start time + the measurement's elapsed time doesn't exceed end_time.
  WHERE UNIX_MICROS(msak.raw.StartTime) + sm.ElapsedTime <= UNIX_MICROS(r.end_time) 
  AND raw.Direction = "download"
  GROUP BY raw.MeasurementID
),

max_bytes_acked_per_stream AS (
  -- Get the last TCPInfo.BytesAcked for snapshots between the first stream that started
  -- and the first stream that terminated.
  SELECT raw.MeasurementID,
    raw.UUID,
    max(sm.TCPInfo.BytesAcked) as max_bytes_acked
    FROM `{{.ProjectID}}.msak.throughput1` msak
      JOIN UNNEST(raw.ServerMeasurements) sm
      JOIN time_range r ON msak.raw.MeasurementID = r.mid
    -- Verify that the test's start time + the measurement's elapsed time doesn't exceed end_time.
    WHERE UNIX_MICROS(msak.raw.StartTime) + sm.ElapsedTime <= UNIX_MICROS(r.end_time)
    AND raw.Direction = "download"
    GROUP BY raw.MeasurementID, raw.UUID
    ORDER BY raw.MeasurementID
)

SELECT
  t1.date,
  raw.MeasurementID,
  SUM(max_bytes_acked) / (
    SELECT
      elapsed
    FROM
      actual_elapsed_times
    WHERE
      mid = raw.MeasurementID
  ) * 8 as ThroughputMbps
FROM
  `{{.ProjectID}}.msak.throughput1` t1
  JOIN max_bytes_acked_per_stream t2 ON t1.raw.MeasurementID = t2.MeasurementID
GROUP BY
  date,
  raw.MeasurementID
