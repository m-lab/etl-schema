--
-- scamper2_joined - joins the raw scamper2 and annotation2 autoloaded datasets
-- with standard columns. This adds the server & client annotations (Geo and
-- Network/ASN) to scamper2 traceroutes so that they can be selected by client
-- or server, matching the annotated scamper1 tables produced by the parser
-- pipeline. Annotations are joined by connection UUID and date.
--
WITH scamper2 AS (
  SELECT
    raw.Metadata.UUID AS id,
    *
  FROM `{{.ProjectID}}.autoload_v2_{{ORG}}_ndt.scamper2_raw`
), ann2 AS (
  SELECT raw.UUID AS id, *
  FROM `{{.ProjectID}}.autoload_v2_{{ORG}}_ndt.annotation2_raw`
)

SELECT
  -- Standard column order.
  scamper2.id,
  scamper2.date,
  scamper2.archiver,
  ann2.raw.server,
  ann2.raw.client,
  scamper2.raw
FROM scamper2 LEFT JOIN ann2
  ON scamper2.id = ann2.id AND scamper2.date = ann2.date
WHERE scamper2.id IS NOT NULL
