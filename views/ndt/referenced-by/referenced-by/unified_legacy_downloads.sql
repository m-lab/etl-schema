--
-- This view, unified_legacy_downloads, was our best view of all MLab data
-- as of 2020-09-14.   Everybody should switch to the updated ndt.unified_downloads
-- as soon as possible.

-- This legacy view might help to ease the transition for live running code,
-- but do not assume that it will exist for more than a few days.
--
SELECT *
EXCEPT (filter)
FROM (
    -- NB: reordering UNION clauses will cause breaking changes to field names
    -- 2019-07-18 to present
    SELECT id, date, a, filter, node, client, server, date AS test_date
    FROM `{{.ProjectID}}.library.ndt_unified_ndt5_downloads`
  UNION ALL
    -- 2020-03-12 to present
    SELECT id, date, a, filter, node, client, server, date AS test_date
    FROM `{{.ProjectID}}.library.ndt_unified_ndt7_downloads`
  UNION ALL
    -- 2009-02-18 to 2019-11-20
    SELECT id, date, a, filter, node, client, server, date AS test_date
    FROM `{{.ProjectID}}.library.ndt_unified_web100_downloads`
)
WHERE
  filter.IsValidBest
