--
-- This view shows decisions1 from raw wehe for users
-- that are not CLI (userID does not start with @)
-- and carried over mobile network
--s
SELECT * FROM `{{.ProjectID}}.raw_wehe.decisions1`
WHERE NOT (raw.userID LIKE '@%') AND (raw.metadata.updatedCarrierName like '%(cellular)')
