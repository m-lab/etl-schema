--
-- This filtered view, ndt7_downloads, is our current best
-- understanding of all representative MLab ndt7 download data.  Rows
-- have been filtered to exclude non-representative tests but the
-- schema is a superset of the Standard Top-level Columns design.
-- Columns that are not part of that design may be subject to breaking
-- changes in the future.
--
-- Researchers are strongly encouraged to use our _unified_ views
-- directly or alternatively copy them to private views or subqueries
-- and editing or augmenting them to as needed to support your
-- research.

-- This filtered view provides additional columns, at the expense of
-- not being unionable with equivalent data from other tools.

-- Your research can be updated more easily your queries are layered:
-- a private version of our unified views (as a view or subquery) to
-- preen and marshal our data according to your needs; and your
-- research query which only uses columns from your private unified
-- view or our standard columns.
--
SELECT *
FROM `{{.ProjectID}}.intermediate_ndt.extended_ndt7_downloads`
WHERE filter.IsValidBest
