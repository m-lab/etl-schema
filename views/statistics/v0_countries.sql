
WITH
countries_2020 AS (
  SELECT * FROM `mlab-oti.statistics.countries_2020`
),
countries_2021 AS (
  SELECT * FROM `mlab-oti.statistics.countries_2021`
),
all_years AS (
  SELECT * FROM countries_2020
  UNION ALL (SELECT * FROM countries_2021)
)
SELECT * FROM all_years