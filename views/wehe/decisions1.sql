--
-- This view shows decisions1 from raw wehe for users
-- that are not CLI (userID does not start with @)
-- and carried over mobile network
--
SELECT result.* 
FROM `{{.ProjectID}}.raw_wehe.decisions1` as result
INNER JOIN `{{.ProjectID}}.wehe_raw.replayInfo1` as info
ON result.raw.userID = info.raw.userID 
   AND CAST(result.raw.historyCount AS INT64) = CAST(info.raw.historyCount AS INT64) 
   AND result.raw.testID = info.raw.testID 
   AND result.date = info.date
WHERE NOT (info.raw.userID LIKE '@%') AND (info.raw.metadata.updatedCarrierName like '%(cellular)')
