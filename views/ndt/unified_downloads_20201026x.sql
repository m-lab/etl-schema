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
SELECT
  id, date, a, node,
  STRUCT (
    client.IP,
    client.Port,
    STRUCT (  -- Map new geo into older production geo
      client.Geo.ContinentCode AS continent_code,
      client.Geo.CountryCode AS country_code,
      client.Geo.CountryCode3 AS country_code3,
      client.Geo.CountryName AS country_name,
      client.Geo.Region AS region,
      -- client.Geo. Subdivision1ISOCode -- OMITED
      -- client.Geo. Subdivision1Name -- OMITED
      -- client.Geo.Subdivision2ISOCode -- OMITED
      -- client.Geo.Subdivision2Name -- OMITED
      client.Geo.MetroCode AS metro_code,
      client.Geo.City AS city,
      client.Geo.AreaCode AS area_code,
      client.Geo.PostalCode AS postal_code,
      client.Geo.Latitude AS latitude,
      client.Geo.Longitude AS longitude,
      client.Geo.AccuracyRadiusKm AS radius
      -- client.Geo.Missing -- Future
    ) AS Geo,
    client.Network
  ) as client,
  STRUCT (
    server.IP,
    server.Port,
    server.Site,
    server.Machine,
    STRUCT (  -- Map new geo into older production geo
      server.Geo.ContinentCode AS continent_code,
      server.Geo.CountryCode AS country_code,
      server.Geo.CountryCode3 AS country_code3,
      server.Geo.CountryName AS country_name,
      server.Geo.Region AS region,
      -- server.Geo. Subdivision1ISOCode -- OMITED
      -- server.Geo. Subdivision1Name -- OMITED
      -- server.Geo.Subdivision2ISOCode -- OMITED
      -- server.Geo.Subdivision2Name -- OMITED
      server.Geo.MetroCode AS metro_code,
      server.Geo.City AS city,
      server.Geo.AreaCode AS area_code,
      server.Geo.PostalCode AS postal_code,
      server.Geo.Latitude AS latitude,
      server.Geo.Longitude AS longitude,
      server.Geo.AccuracyRadiusKm AS radius
      -- server.Geo.Missing -- Future
    ) AS Geo,
    server.Network
  ) as server,
  date as test_date
FROM `{{.ProjectID}}.ndt.unified_downloads`
