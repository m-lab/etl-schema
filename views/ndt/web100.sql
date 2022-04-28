--
-- This view is a pass-through for date partitioned ndt web100 data. The data
-- in this table is a static transformation of data from the v1 data pipeline
-- for the ndt web100 dataset. It is "static" because it is not actively
-- reprocessed. While it uses standard column conventions, the schema is not
-- guaranteed to be backward compatible b/c there is currently no parser that
-- supports reprocessing this format.
--
SELECT * FROM `{{.ProjectID}}.ndt.web100`
