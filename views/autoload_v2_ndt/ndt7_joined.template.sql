WITH ndt7 AS (
  SELECT
    -- Pick the download or upload UUID per row.
    IF(raw.Download IS NOT NULL,
       raw.Download.UUID,
       IF(raw.Upload IS NOT NULL,
          raw.Upload.UUID, NULL)) AS id,
    -- Construct the summary 'a' record for compatibility with standard columns.
    STRUCT (
      IF(raw.Download IS NOT NULL,
         raw.Download.UUID,
         IF(raw.Upload IS NOT NULL, raw.Upload.UUID, NULL)) AS UUID,
      IF(raw.Download IS NOT NULL,
         raw.Download.StartTime,
         IF(raw.Upload IS NOT NULL, raw.Upload.StartTime, NULL)) AS TestTime,
      -- TODO(soltesz): read this from the snapshots instead. Some BYOS nodes may be misconfigured.
      "bbr" AS CongestionControl,
      8 * IF(raw.Download IS NOT NULL,
             ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesAcked / ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.ElapsedTime,
             IF(raw.Upload IS NOT NULL,
                ARRAY_REVERSE(raw.Upload.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesAcked / ARRAY_REVERSE(raw.Upload.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.ElapsedTime, NULL)) AS MeanThroughputMbps,
      IF(raw.Download IS NOT NULL,
         ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.MinRTT,
         IF(raw.Upload IS NOT NULL,
            ARRAY_REVERSE(raw.Upload.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.MinRTT, NULL)) / 1000 AS MinRTT, -- unit: ms
      IF(raw.Download IS NOT NULL,
         ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesRetrans / ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesSent,
         IF(raw.Upload IS NOT NULL,
            ARRAY_REVERSE(raw.Upload.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesRetrans / ARRAY_REVERSE(raw.Upload.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesSent, NULL)) AS LossRate
    ) AS a,
  *
  FROM `mlab-autojoin.autoload_v2_{{ORG}}_ndt.ndt7_raw`
), ann2 AS (
  SELECT raw.UUID AS id, *
  FROM `mlab-autojoin.autoload_v2_{{ORG}}_ndt.annotation2_raw`
)

SELECT
  -- Standard column order.
  ndt7.id,
  ndt7.date,
  ndt7.archiver,
  ann2.raw.server,
  ann2.raw.client,
  ndt7.a,
  ndt7.raw
FROM ndt7 LEFT JOIN ann2
  ON ndt7.id = ann2.id AND ndt7.date = ann2.date
WHERE ndt7.id IS NOT NULL

