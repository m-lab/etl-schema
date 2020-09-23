--
-- This view, ndt_unified_downloads, is our current best understanding
-- of all M-Lab download data across the entire platform over all time.
-- The schema uses the Standard Top-level Columns design.
--
-- This view receives long term support by the M-Lab team.
--
-- It is our intent to avoid any changes to this view that might break
-- dependent queries.  We expect to add columns in the future, but
-- never to delete them.
--
-- Note that as our data and our understanding of it improves, the
-- data under this view will change.  This view will isolate
-- constituent changes from users.
--
-- Views of the form ndt_unified_downloads_XXXX reflect our
-- understanding of the data under different assumptions or at earlier
-- dates.  These views are intended to be useful to test how our
-- processing changed might affect research results.
--
-- Researchers are strongly encouraged to use one of our _unified_
-- views.
--
-- If you must to create your own unified view, we strongly suggest
-- layering your queries such that data preening is done in sub-queries
-- or views, distinct from the research queries, such that the data
-- preening can be updated to follow changes to the constituent views
-- without refactoring the research.
--
-- We do not consider changes to our constituent views to be breaking
-- changes if the changes are fully masked by our unified views.
--
-- NB: deprecate test_date in favor of date
--
SELECT *
EXCEPT (filter)
FROM (
    -- NB: reordering UNION clauses will cause breaking changes to field names
    -- 2019-07-18 to present
    SELECT id, test_date AS date, a, filter, node, client, server, test_date
    FROM `{{.ProjectID}}.library.ndt_unified_ndt5_downloads`
  UNION ALL
    -- 2020-03-12 to present
    SELECT id, test_date AS date, a, filter, node, client, server, test_date
    FROM `{{.ProjectID}}.library.ndt_unified_ndt7_downloads`
  UNION ALL
    -- 2009-02-18 to 2019-11-20
    SELECT id, test_date AS date, a, filter, node, client, server, test_date
    FROM `{{.ProjectID}}.library.ndt_unified_web100_downloads`
)
WHERE
  filter.IsValidBest
