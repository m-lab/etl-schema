#!/bin/bash
#
# create_target_views.sh creates "base" views that reference all data from
# source tables managed by gardener.
#
# Example usage:
#
#  ./create_target_views.sh mlab-sandbox mlab-sandbox "ndt sidestream"
#  ./create_target_views.sh mlab-oti measurement-lab "ndt sidestream"


set -eu
USAGE="$0 <source-project> <dest-project> <experiment1 experiment2 [...]>"
SRC_PROJECT=${1:?Please provide source project: $USAGE}
DST_PROJECT=${2:?Please provide destination project: $USAGE}
EXPERIMENTS=${3:?Please provide set of experiment names: $USAGE}


# create_target_view creates a view at dst_view that selects all content from
# the given src_table.
#
# Args:
#   src_table: source table in the form of "<project>:<dataset>.<table>".
#   dst_view: view destination in the form "<project>:<dataset>.<view>". Dataset
#     should already exist.
#   description: text description of this view.
function create_target_view() {
  local src_table=$1
  local dst_view=$2
  local description="Release tag: $TRAVIS_TAG     Commit: $TRAVIS_COMMIT"$'\n'$3
  local sql='#standardSQL
    SELECT * FROM `'${src_table/:/.}'`'

  echo ${dst_view}
  bq rm --force ${dst_view}
  bq mk --description="${description}" --view="$sql" ${dst_view}

  # This fetches the new table description as json.
  mkdir -p json
  bq show --format=prettyjson ${dst_view} > json/${dst_view}.json
}


for experiment in ${EXPERIMENTS} ; do

  # Make dataset for base views.
  bq mk ${DST_PROJECT}:${experiment} || :

  # Make base view referring to the source table.
  create_target_view \
      ${SRC_PROJECT}:base_tables.${experiment} \
      ${DST_PROJECT}:${experiment}.base \
      'View of all '${experiment}' data processed by the ETL Gardener'

done
