# Schema

Schema includes bigquery schema (json) files, and code associated with
populating bigquery entities.

Parsers in mlab-sandbox, mlab-staging, and mlab-oti target datasets in the same
project.

All parsers write to the `base_tables` dataset.

## NDT

ndt.json contains the schema for the NDT tables. It can be used to
create a new table in mlab-sandbox project by invoking:

    bq --project_id mlab-sandbox mk --time_partitioning_field=log_time \
        --schema schema/ndt.json --clustering_fields client_country,client_asn,client_region,server_asn -t ndt.web100

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

## Update Table Schema in Place

NOTE: schema updates are best used to add new columns. See [modiying table
schemas](https://cloud.google.com/bigquery/docs/managing-table-schemas) for
other use cases.

For example:

```bash
bq update mlab-sandbox:private.sidestream schema/ss.json
```
