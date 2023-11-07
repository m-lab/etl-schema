--
-- This view shows clientXputs1 from raw wehe for users
-- that are not CLI (userID does not start with @)
-- and carried over mobile network
--
SELECT xput.* 
FROM `{{.ProjectID}}.wehe_raw.clientXputs1` as xput
INNER JOIN `{{.ProjectID}}.wehe_raw.replayInfo1` as info
ON xput.raw.userID = info.raw.userID 
   AND CAST(xput.raw.historyCount AS INT64) = CAST(info.raw.historyCount AS INT64)
   AND xput.raw.testID = info.raw.testID 
   AND xput.date = info.date
WHERE NOT (info.raw.userID LIKE '@%') AND (info.raw.metadata.updatedCarrierName like '%(cellular)')