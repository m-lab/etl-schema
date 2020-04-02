--
-- This view, ndt_unified_downloads_2019, reflects our 2019 and
-- earlier understanding of all MLab download data across the entire
-- platform over all time.  Note that this is a reimplementaion of
-- past general advice - it may not exactly match any specific
-- query.
--
-- In the past we have always recommended excluding measurements that
-- had zero packet loss (or zero retransmissions or zero recoveries)
-- as signatures of bottlenecks outside of the network.  This
-- unfortunately also implicitly excluded measurements that exhibit
-- buffer-bloat, because these typically manifest as other non-network
-- bottlenecks such as running out of receiver window.
--
-- This view is to enable researchers to understand how updating our
-- "Best" views of the data might alter pase and current studies.  As
-- we further improve our understanding of the data, we will update
-- the "Best" views, and track the future history as dated views.
-- This view (and other dated views) will be deleted once they are no
-- longer relevant to the research community.
--
-- This view is still in DRAFT and may change in the near future.
--
-- The schema uses the Standard Top-level Columns design.
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

SELECT * EXCEPT (filter)
FROM (
    SELECT test_date, a, filter, node, client, server
    FROM `{{.ProjectID}}.library.ndt_unified_ndt5_downloads`
  UNION ALL
    SELECT test_date, a, filter, node, client, server
    FROM `{{.ProjectID}}.library.ndt_unified_web100_downloads`
)
WHERE
  filter.IsValid2019
