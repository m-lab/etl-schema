# etl-schema
| branch | travis-ci | report-card | coveralls |
|--------|-----------|-----------|-------------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl-schema.svg?branch=master)](https://travis-ci.org/m-lab/etl-schema) | | [![Coverage Status](https://coveralls.io/repos/m-lab/etl-schema/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl-schema?branch=master) |

[![Waffle.io](https://badge.waffle.io/m-lab/etl-schema.svg?title=Ready)](http://waffle.io/m-lab/etl-schema)

MeasurementLab data ingestion pipeline.

### To create e.g., NDT table (should rarely be required!!!):
```bash
bq mk --time_partitioning_type=DAY --schema=schema/ndt_delta.json mlab-sandbox:batch.ndt
```

### To create the private dataset:
```bash
bq mk mlab-oti:private
bq update --source acl/private.acl.json mlab-oti:private
```

### To create the private production NDT table (should rarely be required!!!):
```bash
bq mk --time_partitioning_type=DAY --schema=schema/ss.json mlab-oti:private.sidestream
```

### To update a table's schema in place:
```bash
bq update mlab-sandbox:private.sidestream schema/ss.json
```

## Also see schema/README.md.

