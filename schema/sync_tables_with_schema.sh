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


USAGE="$0 <project> <dataset>"
PROJECT=${1:?Please specify the project name, e.g. "mlab-sandbox": $USAGE}
DATASET=${2:?Please specify the dataset name, e.g. "base_tables": $USAGE}
NODRYRUN=${3:-dryrun} # Run in dryrun mode by default.

set -eu

TEMPDIR=$( mktemp -d )
BASEDIR="$(dirname "$0")"

# Cleanup the temp directory before exiting for any reason.
function cleanup() {
    local rv=$?
    rm -rf "${TEMPDIR}"
    exit $rv
}
trap "cleanup" INT TERM EXIT


for schema_file in `ls "${BASEDIR}"/*.json`; do
    table="$( basename ${schema_file%%.json} )"

    # Try to fetch current schema as JSON.
    if ! bq --project ${PROJECT} show --format=prettyjson \
       --schema ${DATASET}.$table > ${TEMPDIR}/${table}.json ; then

        echo "Creating(${NODRYRUN}): ${PROJECT}:${DATASET}.${table}"
        if [[ "${NODRYRUN}" == "nodryrun" ]] ; then
            bq --project_id ${PROJECT} mk \
              --time_partitioning_type=DAY \
              --schema ${schema_file} -t "${DATASET}.${table}"
        fi

        # We have just created the table, so the schema is guaranteed to match.
        continue
    fi

    # Compare the normalized JSON schema files.
    #
    # NOTE: `diff` alone reports differences that don't matter. So, we use jq
    # to perform a structural equal operation, irrespective of object order.
    # NOTE: the jq query takes in the two files, assumes they're an array,
    # sorts the objects in the array and compares the result.
    jq_filter='($a|(.|arrays)|=sort) as $a|($b|(.|arrays)|=sort) as $b|$a==$b'
    match=$( jq --argfile a "${TEMPDIR}/${table}.json" \
                --argfile b "${BASEDIR}/${table}.json" \
                -n "${jq_filter}" )

    if [[ "${match}" == "false" ]] ; then
        echo "WARNING: remote and local schemas do not match:" >&2
        echo "WARNING: (<) ${PROJECT}:${DATASET}.$table" >&2
        echo "WARNING: (>) ${BASEDIR}/${table}.json" >&2
        diff <( python -m json.tool "${TEMPDIR}/${table}.json" ) \
             <( python -m json.tool "${BASEDIR}/${table}.json" ) || :

        echo "Updating(${NODRYRUN}): ${PROJECT}:${DATASET}.${table}"
        if [[ "${NODRYRUN}" == "nodryrun" ]] ; then
            bq --project_id ${PROJECT} update \
                "${DATASET}.${table}" ${schema_file}
        fi

    else

        # Both match so nothing to do.
        echo "Success(${NODRYRUN}): ${PROJECT}:${DATASET}.$table matches ${table}.json"
    fi
done
