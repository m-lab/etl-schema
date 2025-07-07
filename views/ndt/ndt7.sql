--
-- This view is a pass-through for annotated ndt7 data excluding Providence information.
--
SELECT * EXCEPT ( Parser )  FROM `{{.ProjectID}}.ndt.ndt7`
