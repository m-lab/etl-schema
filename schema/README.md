# Schema

Schema includes bigquery schema (json) files, and code associated with
populating bigquery entities.

Parsers in mlab-sandbox, mlab-staging, and mlab-oti target datasets in the same
project.

All parsers write to the `base_tables` dataset.

## NDT

ndt.json contains the schema for the NDT tables. It can be used to
create a new table in mlab-sandbox project by invoking:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/ndt.json -t base_tables.ndt

ndt_delta.json contains another NDT schema, including a repeated "delta" field,
intended to contain snapshot deltas. To create a new table:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/ndt_delta.json -t base_tables.ndt_delta

## Paris-Traceroute

paris-traceroute.json contains the schema for paris traceroute tables. To
create a new table:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/paris-traceroute.json -t base_tables.traceroute

## Sidestream

sidestream.json contains the schema for sidestream tables.  To create a new table:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/sidestream.json -t base_tables.sidestream

## Switch - DISCO

switch.json contains the schema for DISCO tables. To create a new table:

    bq --project_id mlab-sandbox mk --time_partitioning_type=DAY \
        --schema schema/switch.json -t base_tables.switch
