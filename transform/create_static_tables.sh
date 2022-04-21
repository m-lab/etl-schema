#!/bin/bash
#
# create_static_tables.sh runs a given sql query that creates a target table. If
# that table already exists, no action is taken.
#
# The target table name is taken from the `CREATE TABLE` directive in the given
# query.
#
# Example usage:
#
#  ./create_static_table.sh mlab-sandbox

set -eu
USAGE="$0 <target-project>"
PROJECT=${1:?Please provide target project: $USAGE}

BASEDIR=$( realpath $( dirname "${BASH_SOURCE[0]}" ) )
cd ${BASEDIR}

function create_table() {
    local query_file=${1:?Please provide query file}
    local table=$( grep 'CREATE TABLE' $query_file | awk '{print $3}' )

    bq query --project_id=$PROJECT --nouse_legacy_sql "$( cat $query_file )"
    echo "Created table $PROJECT.$table successfully"
}

create_table ./web100_static.sql
