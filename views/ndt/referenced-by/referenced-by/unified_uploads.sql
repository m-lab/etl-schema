--
-- This view, ndt_unified_uploads, is our current best understanding
-- of all MLab upload data across the entire platform over all time.
-- The schema uses the Standard Top-level Columns design.
--
-- This view is intended to receive long term support by the M-Lab
-- team.
--
-- It is our intent to avoid any changes to this view that might break
-- dependent queries.  We expect to add columns in the future, but
-- never to delete them.
--
-- Note that as our data and our understanding of it improves, the
-- data under this view will change.  This view will isolate
-- constituent changes from users.
--
-- Views of the form ndt_unified_uploads_XXXX reflect our
-- understanding of the data under different assumptions or at earlier
-- dates.  These views are intended to be useful to test how our
-- processing changed might affect research results.
--
-- Researchers are strongly encouraged to use our _unified_ views
-- directly or alternatively copy them to private views or subqueries
-- and editing or augmenting them to as needed to support your
-- research.
--
-- Your research can be updated more easily if your queries are
-- layered: a private version of our unified views (as a view or
-- subquery) to preen and marshal our data according to your needs;
-- and your research query which only uses columns from your private
-- unified view or our standard columns.
--
-- We do not consider changes to our intermediate views to be breaking
-- changes if the changes are fully masked by our unified views.
--
-- NB: deprecate test_date in favor of date
--
SELECT *
EXCEPT (filter)
FROM (
    -- NB: reordering UNION clauses may cause breaking changes to field names
    -- 2020-03-12 to present
    SELECT id, test_date AS date, a, filter, node, client, server, test_date
    FROM `{{.ProjectID}}.intermediate_ndt.extended_ndt7_uploads`
  UNION ALL
    -- 2019-07-18 to present
    SELECT id, test_date AS date, a, filter, node, client, server, test_date
    FROM `{{.ProjectID}}.intermediate_ndt.extended_ndt5_uploads`
  UNION ALL
    -- 2009-02-18 to 2019-11-20
    SELECT id, test_date AS date, a, filter, node, client, server, test_date
    FROM `{{.ProjectID}}.intermediate_ndt.extended_web100_uploads`
)
WHERE
  filter.IsValidBest
