--
-- This view is a pass-through for annotated ndt7 data.  This materializes data
-- from ndt_raw.ndt7 with ndt_raw.annotation2 into a single location.
--
SELECT * FROM `{{.ProjectID}}.ndt.ndt7`
