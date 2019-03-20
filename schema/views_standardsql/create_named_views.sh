#!/bin/bash
#
# create_named_views.sh creates "named" views that reference all data from
# source tables managed by gardener.
#
# Example usage:
#
#  ./create_named_views.sh mlab-sandbox mlab-sandbox \
#       base_tables.ndt=ndt.web100 \
#       base_tables.sidestream=global.sidestream
#  ./create_named_views.sh mlab-oti measurement-lab \
#       base_tables.ndt=ndt.web100 \
#       base_tables.sidestream=global.sidestream

set -eu
USAGE="$0 <env-name> <source-project> <dest-project>"
USAGE+=" <dataset1.table=dataset2.view [...]>"
KEYNAME=${1:?Please provide a key name}
SRC_PROJECT=${2:?Please provide source project: $USAGE}
DST_PROJECT=${3:?Please provide destination project: $USAGE}
shift 3
_=${1:?Please provide set of view assignments: $USAGE}

echo "${!KEYNAME}" > /tmp/sa.json
export GOOGLE_APPLICATION_CREDENTIALS=/tmp/sa.json

for assignment in $@ ; do

  # Extract the source table and destination view from the assignment spec.
  src=${assignment%%=*}
  dest=${assignment##*=}

  # Make dataset for view.
  bq mk "${DST_PROJECT}:${dest%%.*}" || :

  # Make view referring to the source table.
  description="Release tag: $TRAVIS_TAG     Commit: $TRAVIS_COMMIT"$'\n'
  description+="View of all '${SRC_PROJECT}.${src}' data processed by the"
  description+=" ETL Gardener."

  bq_create_view \
      -create-view "${DST_PROJECT}.${dest}" \
      -description "${description}" \
      -to-access "${SRC_PROJECT}.${src}"
done
