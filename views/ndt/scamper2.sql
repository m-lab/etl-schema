--
-- This view is a pass-through for scamper2 traceroute data from autonode deployments.
--
SELECT * FROM `{{.ProjectID}}.autojoin_autoload_v2_ndt.scamper2_union`
