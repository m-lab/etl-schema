#!/bin/bash
#
# create_base_views.sh creates "base" views that reference all data from
# source tables managed by gardener.
#
# Example usage:
#
#  ./create_base_views.sh mlab-sandbox mlab-sandbox "ndt sidestream"
#  ./create_base_views.sh mlab-oti measurement-lab "ndt sidestream"


set -eu
USAGE="$0 <source-project> <dest-project> <experiment1 experiment2 [...]>"
SRC_PROJECT=${1:?Please provide source project: $USAGE}
DST_PROJECT=${2:?Please provide destination project: $USAGE}
EXPERIMENTS=${3:?Please provide set of experiment names: $USAGE}

(set +x ; echo "${SERVICE_ACCOUNT_mlab_sandbox}" > /tmp/sa.json)
export GOOGLE_APPLICATION_CREDENTIALS=/tmp/sa.json

for experiment in ${EXPERIMENTS} ; do

  # Make dataset for base views.
  bq mk "${DST_PROJECT}:${experiment}" || :

  # Make base view referring to the source table.
  description="Release tag: $TRAVIS_TAG     Commit: $TRAVIS_COMMIT\n"
  description+="View of all '${experiment}' data processed by the ETL Gardener"

  bq_create_view \
      -create-view "${DST_PROJECT}.${experiment}.base" \
      -description "${description}" \
      -to-access "${SRC_PROJECT}.base_tables.${experiment}"
done
