--
-- ndt7_joined - joins the raw ndt7 and annotation2 autoloaded datasets with standard columns.
--
WITH prendt7 AS (
  SELECT
    raw.Download IS NOT NULL AS isDownload,
    raw.Upload IS NOT NULL AS isUpload,
    ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].BBRInfo IS NOT NULL AS isBBR,
    ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesAcked AS downloadBytesAcked,
    ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.ElapsedTime AS downloadElapsedTime,
    ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.MinRTT AS downloadMinRTT,
    ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesRetrans AS downloadBytesRetrans,
    ARRAY_REVERSE(raw.Download.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesSent AS downloadBytesSent,
    ARRAY_REVERSE(raw.Upload.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.BytesReceived AS uploadBytesReceived,
    ARRAY_REVERSE(raw.Upload.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.ElapsedTime AS uploadElapsedTime,
    ARRAY_REVERSE(raw.Upload.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.MinRTT AS uploadMinRTT,
    *
  FROM `{{.ProjectID}}.autoload_v2_{{ORG}}_ndt.ndt7_raw`

), ndt7 AS (
  SELECT
    -- Pick the download or upload UUID per row.
    IF(isDownload, raw.Download.UUID, IF(isUpload, raw.Upload.UUID, NULL)) AS id,
    -- Construct the summary 'a' record for compatibility with standard columns.
    STRUCT (
      IF(isDownload, raw.Download.UUID, IF(isUpload, raw.Upload.UUID, NULL)) AS UUID,
      IF(isDownload, raw.Download.StartTime, IF(isUpload, raw.Upload.StartTime, NULL)) AS TestTime,
      IF(isBBR, "bbr", "unknown") AS CongestionControl,
      8 * IF(isDownload, downloadBytesAcked / downloadElapsedTime,
             IF(isUpload,  uploadBytesReceived / uploadElapsedTime, NULL)) AS MeanThroughputMbps,
      IF(isDownload, downloadMinRTT, IF(isUpload, uploadMinRTT, NULL)) / 1000 AS MinRTT, -- unit: ms
      IF(isDownload, downloadBytesRetrans / downloadBytesSent, IF(isUpload, 0, NULL)) AS LossRate
    ) AS a,
  *
  FROM prendt7
), ann2 AS (
  SELECT raw.UUID AS id, *
  FROM `{{.ProjectID}}.autoload_v2_{{ORG}}_ndt.annotation2_raw`
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

