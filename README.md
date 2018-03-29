# etl-schema
| branch | travis-ci | report-card | coveralls |
|--------|-----------|-----------|-------------|
| master | [![Travis Build Status](https://travis-ci.org/m-lab/etl-schema.svg?branch=master)](https://travis-ci.org/m-lab/etl-schema) | | [![Coverage Status](https://coveralls.io/repos/m-lab/etl-schema/badge.svg?branch=master)](https://coveralls.io/github/m-lab/etl-schema?branch=master) |

[![Waffle.io](https://badge.waffle.io/m-lab/etl-schema.svg?title=Ready)](http://waffle.io/m-lab/etl-schema)

MeasurementLab data ingestion pipeline.

To create e.g., NDT table (should rarely be required!!!):
bq mk --time_partitioning_type=DAY --schema=schema/repeated.json mlab-sandbox:mlab_sandbox.ndt

Also see schema/README.md.

