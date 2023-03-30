--
-- This view is a pass-through for annotated tcpinfo data.  This materializes data
-- from ndt_raw.tcpinfo with ndt_raw.annotation2 into a single location.
--
SELECT * FROM `{{.ProjectID}}.ndt.tcpinfo`
