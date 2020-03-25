#StandardSQL
-- This view, ndt_unified_uploads, is our current best understanding
-- of all MLab upload data across the entire platform over all time.
-- The resulting schema uses field names defined by the Standard
-- Top-level Columns design. This schema is intended to receive long
-- term support.
--
-- It is our intent to avoid any changes to this view that might break
-- dependent queries.  We expect to add columns in the future, but
-- never to delete them.
--
-- Note that as our data and our understanding of it improves, the
-- data under this view will change.  This view will isolate
-- constituent changes from users.
--
-- Views of the form ndt_unified_uploads_retroXXXX reflect our
-- understanding of the data under different assumptions or at earlier
-- dates.  These view might be useful to test how our processing
-- changed might affect older research results.
--
-- Researchers are strongly encouraged to use one of our _unified_
-- views.  If you must to create your own, we strongly suggest
-- layering your query to partition the research questions from data
-- reformatting issues.  We will not consider changes to or
-- constituent views to be breaking changes if the changes are fully
-- masked by our unified views.


 -- Export the result of the union of both tables.
SELECT * EXCEPT (b)
FROM (
    SELECT test_date, a, b, client, server
    FROM `{{.ProjectID}}.library.ndt_unified_ndt5_uploads`
  UNION ALL
    SELECT test_date, a, b, client, server
    FROM `{{.ProjectID}}.library.ndt_unified_web100_uploads`
)
WHERE
  b.row_valid_best
