#standardSQL
-- Combined view of plx legacy sidestream table, up to May 10, and
-- view of unembargoed rows from new ETL table, from May 10, 2017 onward.
SELECT _PARTITIONTIME as partition_date, *
FROM `${PROJECT}.private.sidestream`
WHERE (task_filename NOT LIKE "%-e.tgz%"
OR TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), log_time, HOUR) > 365*24)
UNION ALL
SELECT *
FROM `${PROJECT}.legacy.sidestream_2015_2017`
UNION ALL
SELECT *
FROM `${PROJECT}.legacy.sidestream_pre2015`
