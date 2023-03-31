--
-- This view is a pass-through for annotated scamper1 data. This materializes
-- data from ndt_raw.scamper1 with ndt_raw.annotation2 into a single location.
--
SELECT * FROM `{{.ProjectID}}.ndt.scamper1`
