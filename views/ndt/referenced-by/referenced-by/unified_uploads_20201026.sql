--
-- This legacy view recreates the similarly named standard view as of
-- the date appearing in the view name.
--
-- The standard view has been improved in ways that might break
-- previously correct queries.  We are providing this legacy view to
-- give users more time update their queries.
--
-- By default, we plan to support this legacy view for two weeks, but
-- may adjust that on a case-by-case basis.
--
-- This is a temporary workaround for deferring the following changes:
--
-- Change test_date to date (both are supported here).
--
-- Change the following field names in *.Geo:
--  continent_code to ContinentCode
--    country_code to CountryCode
--   country_code3 to CountryCode3
--    country_name to CountryName
--          region to Region
--      metro_code to MetroCode
--            city to City
--       area_code to AreaCode
--     postal_code to PostalCode
--        latitude to Latitude
--       longitude to Longitude
--          radius to AccuracyRadiusKm
--
SELECT *
EXCEPT (filter)
FROM (
    -- NB: reordering UNION clauses may cause breaking changes to field names
    -- 2019-07-18 to present
    SELECT id, date, a, filter, node, client, server, date AS test_date
    FROM `{{.ProjectID}}.library.ndt_unified_ndt5_uploads`
  UNION ALL
    -- 2020-03-12 to present
    SELECT id, date, a, filter, node, client, server, date AS test_date
    FROM `{{.ProjectID}}.library.ndt_unified_ndt7_uploads`
  UNION ALL
    -- 2009-02-18 to 2019-11-20
    SELECT id, date, a, filter, node, client, server, date AS test_date
    FROM `{{.ProjectID}}.library.ndt_unified_web100_uploads`
)
WHERE
  filter.IsValidBest
