#!/bin/bash
#
# sync_tables_with_schema.sh creates or updates BQ tables using local schema
# definitions. If the remote schema for an existing table is structurally
# different than the local schema, the difference is printed before being
# updated.
#
# By default, sync_tables_with_schema.sh run in dryrun mode, making no
# permanent changes. To commit changes, provide the final argument 'nodryrun'.
#
# Example:
#   ./sync_tables_with_schema.sh mlab-sandbox batch [nodryrun]


USAGE="$0 <project>"
PROJECT=${1:?Please specify the project name, e.g. "mlab-sandbox": $USAGE}
NODRYRUN=${2:-dryrun} # Run in dryrun mode by default.

set -eu

# NOTE: the bq cli leverages the gcloud auth, however still must perform an
# authentication initialization on the first run. This initialization also
# generates an unconditional "Welcome to BigQuery!" preamble message, which
# corrupts the remaining json output. The following command attempts to list a
# fake dataset which runs through the auth initialization and welcome message.
bq --headless --project ${PROJECT} ls fake-dataset &> /dev/null || :

bq mk ${PROJECT}:ndt ${PROJECT}:ndt_batch ${PROJECT}:sidestream ${PROJECT}:sidestream_batch
bq mk ${PROJECT}:traceroute ${PROJECT}:traceroute_batch ${PROJECT}:switch ${PROJECT}:switch_batch

./sync_table ${PROJECT}:ndt.web100 ndt.json
./sync_table ${PROJECT}:ndt_batch.web100 ndt.json
./sync_table ${PROJECT}:sidestream.web100 sidestream.json
./sync_table ${PROJECT}:sidestream_batch.web100 sidestream.json
./sync_table ${PROJECT}:traceroute.web100 traceroute.json
./sync_table ${PROJECT}:traceroute_batch.web100 traceroute.json
./sync_table ${PROJECT}:switch.web100 switch.json
./sync_table ${PROJECT}:switch_batch.web100 switch.json
