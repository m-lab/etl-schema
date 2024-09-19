SELECT * FROM `{{.ProjectID}}.autoload_v2_mlab_ndt.ndt7_joined`
UNION ALL
SELECT * FROM `{{.ProjectID}}.autoload_v2_rnp_ndt.ndt7_joined`
