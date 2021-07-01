
WITH
global_asn_2020 AS (
  SELECT * FROM `mlab-oti.statistics.global_asn_2020`
),
global_asn_2021 AS (
  SELECT * FROM `mlab-oti.statistics.global_asn_2021`
),
all_years AS (
  SELECT * FROM global_asn_2020
  UNION ALL (SELECT * FROM global_asn_2021)
)
SELECT * FROM all_years