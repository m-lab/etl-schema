--
-- This view, intermediate_ndt.joined_ndt7.sql, contains the output
-- from the raw parser, joined with various annotations.  At this
-- stage nothing has been removed, and many rows contain invalid or
-- corrupt data.
--
-- Unless you are researching the provenance of our data, we strongly
-- encourage you start from the unified views.

SELECT *
FROM  `mlab-oti.ndt.ndt7` -- TODO move to mlab-oti.intermediate_ndt.joined_ndt7
