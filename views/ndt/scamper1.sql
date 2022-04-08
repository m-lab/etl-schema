--
-- This view is a pass-through for annotated scamper1 data. This materializes
-- data from ndt_raw.scamper1 with ndt_raw.annotation into a single location.
--
-- This table includes server and client annotations but not hop annotations. For
-- recent hop annotations see: `ndt.scamper1_hopannotation1`.
--
SELECT * FROM `{{.ProjectID}}.ndt.scamper1`
