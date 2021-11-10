--
-- This view, scamper1_hopannotation1, is an approach to provide annotations
-- (geographic and network location information) for hops obtained from
-- parsing scamper traceroutes.
-- The aim is to gather feedback on the usefulness of such a view.
--
-- The schema uses the Standard Top-level Columns design.
--
-- This view is not intended to receive long term support by the M-Lab
-- team.
--
-- Note that as our data and our understanding of it improves, the
-- data under this view could change.
--
-- Researchers are strongly encouraged to use this view
-- to support their research and provide feedback.
--
WITH scamper1 AS (
    SELECT * FROM `{{.ProjectID}}.ndt.scamper1`
    WHERE date BETWEEN "2021-11-01" AND "2021-11-07"
),

hops AS (
    SELECT scamper1.id, fh AS hop
    FROM scamper1 CROSS JOIN UNNEST(scamper1.raw.Tracelb.nodes) as fh
	WHERE date BETWEEN "2021-11-01" AND "2021-11-07"
),

# Annotate.
annotated AS (
    SELECT hops.id,
	STRUCT(hops.Hop.hop_id, hops.Hop.addr, hops.Hop.name, hops.Hop.q_ttl, hops.Hop.linkc, hops.Hop.links, ann.raw.Annotations AS annotations) as hop
    FROM hops JOIN `{{.ProjectID}}.raw_ndt.hopannotation1` as ann ON (hops.hop.hop_id = ann.id)
),

# Now reassemble the Hop arrays.
mash AS (
    SELECT id, ARRAY_AGG(hop) as hop
    FROM annotated
    GROUP BY id
)

# Recombine the hop arrays with top level fields.
SELECT scamper1.* REPLACE (
    (SELECT AS STRUCT scamper1.raw.* REPLACE (
        (SELECT AS STRUCT raw.Tracelb.* EXCEPT (nodes), mash.hop AS nodes
        ) AS Tracelb)
    ) AS raw
)
FROM scamper1 JOIN mash ON (scamper.id = mash.id)

