-- **** This is a lightly tested prototype for new filter columns ****
-- It may have undetected bugs and is subject to updates without notification.
-- Contact mattmathis
-- ******************************************************* XXX debugging
--
-- This view, ndt_unified_downloads, is our current best understanding
-- of all NDT upload data across the entire platform over all time.
-- The schema follows our Standard Top-level Columns design.
--
-- Every column exposed in this view is intended to receive long term
-- support by the M-Lab team.  It is our intent to minimize any
-- changes to this view that might break dependent queries.  We expect
-- to add columns in the future, but deleting or changing columns will
-- go through a public review cycle.
--
-- Views of the form ndt_unified_downloads_XXXX reflect our
-- understanding of the data under different assumptions or at earlier
-- dates.  These views are intended to be useful to test if our
-- processing changed might have affected any research results.  These
-- views will be supported as long as the underlying data does not
-- change too much.  At some point they are will become unsupportable
-- and be removed.
--
-- Researchers doing studies outside the scope of the unified views
-- are strongly encouraged to copy the Unified Extended subquery below
-- as a private subquery or save them as  private Custom Unified Views
-- (requires a BQ account) and edit them to fill their needs.
--
-- Your research can be updated more easily if your queries are
-- layered: a private custom unified subquery (or a view) to preen and
-- marshal our data according to your needs; and your research query
-- which only uses columns from your private unified view or our
-- standard columns.
--
-- We do not consider changes to our intermediate views to be breaking
-- changes if the changes are fully masked by our unified views.
--
--

WITH

UnifiedExtendedDownloads AS (
  SELECT *,

    -- IsValidBest is our current understanding of best filter for
    -- Studying internet performance
    (
      filter.IsComplete # Not missing any important fields
      AND filter.IsProduction # not a test server
      AND NOT filter.IsErrored # Server reported an error
      AND NOT filter.IsOAM # operations and management traffic
      AND NOT filter.IsPlatformAnomaly # overload, bad version, etc
      AND NOT filter.IsShort # less than 8kB data
      AND NOT filter.IsAborted # insufficient duration
      AND NOT filter.IsHung # excessive duraton
      AND NOT filter._IsRFC1918 # XXX Why traffic from RFC1918 addresses?
--    AND  ( filter._IsCongested OR filter._IsBloated ) Delta relative to IsValid2021
    ) AS IsValidBest,

    -- IsValid2021 was our understading prior to 2022-04-01
    (
      filter.IsComplete # Not missing any important fields
      AND filter.IsProduction # not a test server
      AND NOT filter.IsErrored # Server reported an error
      AND NOT filter.IsOAM # operations and management traffic
      AND NOT filter.IsPlatformAnomaly # overload, bad version, etc
      AND NOT filter.IsShort # less than 8kB data
      AND NOT filter.IsAborted # insufficient duration
      AND NOT filter.IsHung # excessive duraton
      AND NOT filter._IsRFC1918 # Internal network
      AND  ( filter._IsCongested OR filter._IsBloated )
    ) AS IsValid2021,
  FROM (
      -- NDT7: 2020-03-12 to present
      SELECT id, date, a, filter, metadata, client, server,
      FROM `{{.ProjectID}}.ndt_intermediate.extended_ndt7_downloads`
    UNION ALL
      -- NDT5: 2019-07-18 to present
      SELECT id, date, a, filter, metadata, client, server,
      FROM `{{.ProjectID}}.ndt_intermediate.extended_ndt5_downloads`
    UNION ALL
      -- Web100: 2009-02-18 to 2019-11-20 -- delete this clause if you don't need it
      SELECT id, date, a, filter, metadata, client, server,
      FROM `{{.ProjectID}}.ndt_intermediate.extended_web100_downloads`
  )
)

-- Remove the code below to create your own Custom Unified View
SELECT * EXCEPT ( filter )
FROM UnifiedExtendedDownloads
WHERE IsValidBest
