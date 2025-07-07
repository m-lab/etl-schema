--
-- This view is a pass-through for ndt7 data from the autojoin fleet
--
SELECT * FROM `{{.ProjectID}}.autojoin_autoload_v2_ndt.ndt7_union`
