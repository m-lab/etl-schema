
WITH
cities_2020 AS (
  SELECT * FROM `mlab-oti.statistics.cities_2020`
),
cities_2021 AS (
  SELECT * FROM `mlab-oti.statistics.cities_2021`
),
all_years AS (
  SELECT * FROM cities_2020
  UNION ALL (SELECT * FROM cities_2021)
)
SELECT * FROM all_years