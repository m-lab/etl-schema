
WITH
continents_2020 AS (
  SELECT * FROM `mlab-oti.statistics.continents_2020`
),
continents_2021 AS (
  SELECT * FROM `mlab-oti.statistics.continents_2021`
),
all_years AS (
  SELECT * FROM continents_2020
  UNION ALL (SELECT * FROM continents_2021)
)
SELECT * FROM all_years