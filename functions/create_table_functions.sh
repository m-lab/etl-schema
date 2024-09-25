#!/bin/bash
#
# create_table_functions.sh applies all sql files in the current directory to
# the named project.
#
# The input files should include definitions for the target dataset and table function name.
#
# Example usage: ./create_table_functions.sh mlab-sandbox

set -eu
USAGE="$0 <target-project>"
PROJECT=${1:?Please provide target project: $USAGE}

BASEDIR=$( realpath $( dirname "${BASH_SOURCE[0]}" ) )
cd ${BASEDIR}

function create_table_function() {
    local query_file=${1:?Please provide query file}
    cat $query_file | bq query --project_id=$PROJECT --nouse_legacy_sql
}

# NOTE: deploy legacy names last, since they depend on the sql files with suffix.
for sqlfile in `ls *_{autonode,managed,complete}.sql` `ls *.sql | grep -v -E 'autonode|managed|complete'` ; do
    echo "Deploying table function: $sqlfile"
    create_table_function $sqlfile
done
