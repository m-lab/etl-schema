--
-- ndt7_joined - joins the raw ndt7 and annotation2 autoloaded datasets with standard columns.
--
WITH prendt7 AS (
  SELECT
    -- Test leaf fields, not whole structs, so BigQuery can prune unread columns.
    raw.Download.UUID IS NOT NULL AS isDownload,
    raw.Upload.UUID IS NOT NULL AS isUpload,
    -- Last measurement of each array, selected without materializing the whole array of structs.
    (SELECT AS STRUCT m.BBRInfo IS NOT NULL AS isBBR, m.TCPInfo.BytesAcked, m.TCPInfo.ElapsedTime, m.TCPInfo.MinRTT, m.TCPInfo.BytesRetrans, m.TCPInfo.BytesSent
       FROM UNNEST(raw.Download.ServerMeasurements) m WITH OFFSET o ORDER BY o DESC LIMIT 1) AS dl,
    (SELECT AS STRUCT m.TCPInfo.BytesReceived, m.TCPInfo.ElapsedTime, m.TCPInfo.MinRTT
       FROM UNNEST(raw.Upload.ServerMeasurements) m WITH OFFSET o ORDER BY o DESC LIMIT 1) AS ul,
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
      IF(IFNULL(dl.isBBR, FALSE), "bbr", "unknown") AS CongestionControl,
      8 * IF(isDownload, dl.BytesAcked / dl.ElapsedTime,
             IF(isUpload,  ul.BytesReceived / ul.ElapsedTime, NULL)) AS MeanThroughputMbps,
      IF(isDownload, dl.MinRTT, IF(isUpload, ul.MinRTT, NULL)) / 1000 AS MinRTT, -- unit: ms
      IF(isDownload, dl.BytesRetrans / dl.BytesSent, IF(isUpload, 0, NULL)) AS LossRate
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

