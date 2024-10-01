WITH
test_time_range AS (
  SELECT raw.MeasurementID as mid, min(raw.StartTime) as test_start_time, min(raw.EndTime) as test_end_time
  FROM `{{.ProjectID}}.msak.throughput1`
  WHERE
   date BETWEEN "2024-01-01" AND CURRENT_DATE
  AND raw.Direction = "download"
  GROUP BY raw.MeasurementID
  
  -- Ignore tests longer than 60s.
  HAVING TIMESTAMP_DIFF(test_end_time, test_start_time, SECOND) <= 60
),

-- Debug subquery - Get all streams' snapshots for a single MeasurementID.
-- stream_snapshots AS (
--   SELECT raw.MeasurementID, raw.UUID, sm.TCPInfo.BytesAcked, sm.ElapsedTime
--     FROM `{{.ProjectID}}.msak.throughput1` msak
--       JOIN UNNEST(raw.ServerMeasurements) sm
--       JOIN test_time_range r ON msak.raw.MeasurementID = r.mid
--     -- Verify that the test's start time + the measurement's elapsed time doesn't exceed end_time.
--     WHERE
--       UNIX_MICROS(msak.raw.StartTime) + sm.ElapsedTime <= UNIX_MICROS(r.test_end_time)
--       AND date BETWEEN "2024-01-01" AND CURRENT_DATE
--       AND msak.raw.MeasurementID = "15614e5d-6e3b-4e60-bf33-9d488dad06b3"
--     AND raw.Direction = "download"
-- ),

stream_bytes_acked AS (
  -- Get the last TCPInfo.BytesAcked for snapshots between the first stream that started
  -- and the first stream that terminated.
  SELECT a.MeasurementId,
    a.UUID,
    date,
    ANY_VALUE(client) as client,
    ANY_VALUE(a.CongestionControl) as cc,
    r.test_start_time as StartTime,
    r.test_end_time as EndTime,
    max(sm.ElapsedTime) as elapsed,
    max(sm.TCPInfo.BytesAcked) as max_bytes_acked
    FROM `{{.ProjectID}}.msak.throughput1` msak
      JOIN UNNEST(raw.ServerMeasurements) sm
      JOIN test_time_range r ON msak.raw.MeasurementID = r.mid
    -- Verify that the test's start time + the measurement's elapsed time doesn't exceed end_time.
    WHERE
      UNIX_MICROS(msak.raw.StartTime) + sm.ElapsedTime <= UNIX_MICROS(r.test_end_time)
      AND date BETWEEN "2024-01-01" AND CURRENT_DATE
    AND raw.Direction = "download"
    GROUP BY a.MeasurementID, a.UUID, date, r.test_start_time, r.test_end_time
)

-- SELECT * FROM stream_snapshots 

SELECT
  MeasurementID as id,
  date,
  STRUCT (
    StartTime,
    EndTime,
    SUM(max_bytes_acked) / MAX(elapsed) * 8 as ThroughputMbps,
    COUNT(*) as NumStreams,
    ANY_VALUE(cc) as CongestionControl
  ) as a,
  ANY_VALUE(client) as client
FROM stream_bytes_acked
GROUP BY MeasurementID, date, StartTime, EndTime
