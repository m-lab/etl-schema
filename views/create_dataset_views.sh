#!/bin/bash
#
# create_dataset_views.sh creates all datasets and views within the current
# working directory. Every directory is a dataset name and every sql file
# within the dataset subdirectory should be a view query template.
#
# Additional subdirectories named 'referenced-by' may also contain view query
# templates that reference views in the same project as the views one level
# above it.
#
# Example usage:
#
#  ./create_dataset_views.sh "self" mlab-sandbox mlab-sandbox
#  ./create_dataset_views.sh "self" mlab-oti measurement-lab

set -eu
USAGE="$0 <key-name> <source-project> <dest-project>"
KEYNAME=${1:?Please provide a key name to authorize operations or "self"}
SRC_PROJECT=${2:?Please provide source project: $USAGE}
DST_PROJECT=${3:?Please provide destination project: $USAGE}

BASEDIR=$( pwd )

if [[ "${KEYNAME}" != "self" ]] ; then
  echo "${!KEYNAME}" > /tmp/sa.json
  export GOOGLE_APPLICATION_CREDENTIALS=/tmp/sa.json
  # Guarantee that `gcloud config get-value accounnt` works as intended.
  gcloud auth activate-service-account --key-file /tmp/sa.json
fi
# Extract service account user name.
USER=$( gcloud config get-value account )


function create_view() {
  local src_project=$1
  local dst_project=$2
  local dataset=$3
  local template=$4

  description=$(
    awk '/^--/ {print substr($0, 3)} /^SELECT/ {exit(0)}' ${template} )
  description+=$'\n'$'\n'"Release tag: $TRAVIS_TAG     Commit: $TRAVIS_COMMIT"
  description+=$'\n'"View of data from '${src_project}'."

  # Strip filename down to view name.
  view="${template%%.sql}"
  view="${view##./}"

  echo -n "Creating "${dst_project}.${dataset}.${view}" using "${template}

  bq_create_view \
      -src-project "${src_project}" \
      -create-view "${dst_project}.${dataset}.${view}" \
      -template "${template}" \
      -description "${description}" \
      -editor "${USER}"
}


# For each directory in the current directory.
for DATASET_DIR in $( find -maxdepth 1 -type d -a -not -name "." ) ; do
  pushd $DATASET_DIR &> /dev/null

    DATASET=${DATASET_DIR##./}

    # Create top level views. These reference tables in the "SRC_PROJECT".
    for TEMPLATE in $( find -maxdepth 1 -name "*.sql" ) ; do

      create_view "${SRC_PROJECT}" "${DST_PROJECT}" "${DATASET}" "${TEMPLATE}"
    done

    # Create all views that reference the top level views. These always
    # reference the same DST_PROJECT.
    while [[ -d "referenced-by" ]] ; do

      cd referenced-by
      for TEMPLATE in $( find -maxdepth 1 -name "*.sql" ) ; do

        create_view "${DST_PROJECT}" "${DST_PROJECT}" "${DATASET}" "${TEMPLATE}"
      done
    done

  popd &> /dev/null
done
