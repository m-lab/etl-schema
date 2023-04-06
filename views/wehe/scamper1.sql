--
-- This view is a pass-through for annotated scamper1 data. This materializes
-- data from wehe_raw.scamper1 with wehe_raw.annotation2 into a single location.
--
SELECT * FROM `{{.ProjectID}}.wehe.scamper1`
