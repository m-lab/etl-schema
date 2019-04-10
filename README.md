# etl-schema

| branch | travis-ci | report-card | coveralls |
|--------|-----------|-----------|-------------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl-schema.svg?branch=master)](https://travis-ci.org/m-lab/etl-schema) | | [![Coverage Status](https://coveralls.io/repos/m-lab/etl-schema/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl-schema?branch=master) |

Schema definitions for the MeasurementLab data ingestion pipeline.

## To create the private dataset

```bash
bq mk mlab-oti:private
bq update --source acl/private.acl.json mlab-oti:private
```

## To create the private table (should rarely be required!!!)

```bash
bq mk --time_partitioning_type=DAY --schema=schema/ss.json \
   mlab-oti:private.sidestream
```

## To update a table's schema in place

NOTE: schema updates are best used to add new columns. See [modiying table
schemas](https://cloud.google.com/bigquery/docs/managing-table-schemas) for
other use cases.

```bash
bq update mlab-sandbox:private.sidestream schema/ss.json
```

## Also see schema/README.md

[schema/README.md](schema/README.md)
