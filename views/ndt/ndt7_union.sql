--
-- This view is a union pass-through for annotated ndt7 data.
-- It materializes data from the legacy and autojoin fleets in one location
--
SELECT * EXCEPT ( archiver ) FROM  `{{.ProjectID}}.autojoin_autoload_v2_ndt.ndt7_union`
  UNION ALL
SELECT * EXCEPT ( Parser ) FROM `{{.ProjectID}}.ndt.ndt7`
