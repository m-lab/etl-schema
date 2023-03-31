--
-- This view is a pass-through for annotated ndt5 data.  This materializes data
-- from ndt_raw.ndt5 with ndt_raw.annotation2 into a single location.
--
SELECT * FROM `{{.ProjectID}}.ndt.ndt5`
