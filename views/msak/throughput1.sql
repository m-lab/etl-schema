--
-- This view contains annotated throughput1 data.  This materializes data
-- from msak_raw.throughput1 with msak_raw.annotation2 into a single location.
--
SELECT
    raw.UUID as id,
    t1.date as date,
    STRUCT(raw.StartTime as StartTime,
        raw.EndTime as EndTime,
        raw.MeasurementID as MeasurementID,
        raw.UUID as UUID,
        raw.Direction as Direction,
        raw.CCAlgorithm as CongestionControl) as a,
    archiver,
    server,
    client,
    raw
FROM
    `{{.ProjectID}}.msak_raw.throughput1` t1
    JOIN `{{.ProjectID}}.msak_raw.annotation2` t2 ON t1.raw.UUID = t2.id
