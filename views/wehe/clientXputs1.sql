--
-- This view shows clientXputs1 from raw wehe for users
-- that are not CLI (userID does not start with @)
-- and carried over mobile network
--
SELECT * FROM `{{.ProjectID}}.raw_wehe.clientXputs1`
WHERE NOT (raw.userID LIKE '@%') AND (raw.metadata.updatedCarrierName like '%(cellular)')