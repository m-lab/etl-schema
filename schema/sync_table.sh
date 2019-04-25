#!/bin/bash
#
# sync_table.sh creates or updates a single BQ table using local schema
# definitions. If the remote schema for an existing TABLE is structurally
# different than the local schema, the difference is printed before being
# updated.
#
# By default, sync_table.sh run in dryrun mode, making no
# permanent changes. To commit changes, provide the final argument 'nodryrun'.
#
# Example:
#   ./sync_table.sh mlab-sandbox:ndt.web100 ndt.json [nodryrun]


USAGE="$0 <full-TABLE-name> <schema file>"
FULL_TABLE=${1:?Please specify the full TABLE name, e.g. "mlab-sandbox:ndt.web100": $USAGE}
SCHEMA_FILE=${2:?Please specify the full schema name, e.g. "ndt.json": $USAGE}
NODRYRUN=${3:-dryrun} # Run in dryrun mode by default.

set -eu

[[ ${FULL_TABLE} =~ (.*):(.*)\.(.*) ]]    # parse the project:dataset.table string

PROJECT=${BASH_REMATCH[1]}
DATASET=${BASH_REMATCH[2]}
TABLE=${BASH_REMATCH[3]}

echo ${PROJECT} ${DATASET} ${TABLE}

# Cleanup the temp directory before exiting for any reason.
function cleanup() {
    local rv=$?
    rm -rf "${TEMPDIR}"
    exit $rv
}
trap "cleanup" INT TERM EXIT

TEMPDIR=$( mktemp -d )
BASEDIR="$(dirname "$0")"

# NOTE: the bq cli leverages the gcloud auth, however still must perform an
# authentication initialization on the first run. This initialization also
# generates an unconditional "Welcome to BigQuery!" preamble message, which
# corrupts the remaining json output. The following command attempts to list a
# fake dataset which runs through the auth initialization and welcome message.
bq --headless --project ${PROJECT} ls fake-dataset &> /dev/null || :


# Try to fetch current schema as JSON.
if ! bq --project ${PROJECT} show --format=prettyjson \
   --schema ${DATASET}.${TABLE} > ${TEMPDIR}/${TABLE}.json ; then

    echo "Creating(${NODRYRUN}): ${PROJECT}:${DATASET}.${TABLE}"
    set -x
    if [[ "${NODRYRUN}" == "nodryrun" ]] ; then
        bq --project_id ${PROJECT} mk \
          --time_partitioning_field=log_time \
          --clustering_fields=client_country,client_asn \
          --schema ${SCHEMA_FILE} -t "${DATASET}.${TABLE}"
    fi

    # We have just created the TABLE, so the schema is guaranteed to match.
else

    # Compare the normalized JSON schema files.
    #
    # NOTE: `diff` alone reports differences that don't matter. So, we use jq
    # to perform a structural equal operation, irrespective of object order.
    # NOTE: the jq query takes in the two files, assumes they're an array,
    # sorts the objects in the array and compares the result.
    jq_filter='($a|(.|arrays)|=sort) as $a|($b|(.|arrays)|=sort) as $b|$a==$b'
    match=$( jq --argfile a "${TEMPDIR}/${TABLE}.json" \
                --argfile b "${SCHEMA_FILE}" \
                -n "${jq_filter}" )

    if [[ "${match}" == "false" ]] ; then
        echo "WARNING: remote and local schemas do not match:" >&2
        echo "WARNING: (<) ${PROJECT}:${DATASET}.${TABLE}" >&2
        echo "WARNING: (>) ${SCHEMA_FILE}" >&2
        diff <( python -m json.tool "${TEMPDIR}/${TABLE}.json" ) \
         <( python -m json.tool "${SCHEMA_FILE}" ) || :

        echo "Updating(${NODRYRUN}): ${PROJECT}:${DATASET}.${TABLE}"
        set -x
        if [[ "${NODRYRUN}" == "nodryrun" ]] ; then
            bq update "${FULL_TABLE}" ${SCHEMA_FILE}
        fi

    else

        # Both match so nothing to do.
        echo "Success(${NODRYRUN}): ${PROJECT}:${DATASET}.$TABLE matches ${TABLE}.json"
    fi

fi
