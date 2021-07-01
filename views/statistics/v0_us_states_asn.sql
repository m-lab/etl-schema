
WITH
us_states_asn_2020 AS (
  SELECT * FROM `mlab-oti.statistics.us_states_asn_2020`
),
us_states_asn_2021 AS (
  SELECT * FROM `mlab-oti.statistics.us_states_asn_2021`
),
all_years AS (
  SELECT * FROM us_states_asn_2020
  UNION ALL (SELECT * FROM us_states_asn_2021)
)
SELECT * FROM all_years