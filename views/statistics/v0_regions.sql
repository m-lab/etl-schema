
WITH
regions_2020 AS (
  SELECT * FROM `mlab-oti.statistics.regions_2020`
),
regions_2021 AS (
  SELECT * FROM `mlab-oti.statistics.regions_2021`
),
all_years AS (
  SELECT * FROM regions_2020
  UNION ALL (SELECT * FROM regions_2021)
)
SELECT * FROM all_years