#StandardSQL

SELECT "ndt5" as t, COUNT(*) FROM `mlab-oti.ndt.ndt5`
UNION ALL
SELECT "tcpinfo" as t, COUNT(*) FROM `mlab-oti.base_tables.tcpinfo`
UNION ALL
SELECT "traceroute" as t, COUNT(*) FROM `mlab-oti.batch.traceroute`
UNION ALL
SELECT "web100" as t, COUNT(*) FROM `mlab-oti.ndt.web100`
