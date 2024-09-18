# Library for creating dataset views.

function create_view_init() {
  # Git info is nominally exported from the caller
  if [ -z "${TAG_NAME-}" -o -z "${COMMIT_SHA-}" ]; then
    echo "Not Git"
    export TAG_NAME="manual"
    export COMMIT_SHA="undefined"
  fi

  if [[ "${KEYNAME}" != "self" ]] ; then
    echo "${!KEYNAME}" > /tmp/sa.json
    export GOOGLE_APPLICATION_CREDENTIALS=/tmp/sa.json
    # Guarantee that `gcloud config get-value accounnt` works as intended.
    gcloud auth activate-service-account --key-file /tmp/sa.json
  fi
  # Extract service account user name.
  USER=$( gcloud config get-value account )

  BQ_CREATE_VIEW=bq_create_view
  if [[ -x ./bq_create_view ]] ; then
    BQ_CREATE_VIEW=./bq_create_view
  fi
}

function create_view() {
  local src_project=$1
  local dst_project=$2
  local dataset=$3
  local template=$4

  description=$(
    awk '/^--/ {print substr($0, 3)} /^SELECT/ {exit(0)}' ${template} )
  description+=$'\n'$'\n'"Release tag: $TAG_NAME Commit: $COMMIT_SHA"
  description+=$'\n'"View of data from '${src_project}'."
  description+=$'\n'"Using: github.com/m-lab/..${template}"
  description+=$'\n'"On :"`date`

  # Strip filename down to view name.
  # Note that _nofilter views are generated with .SQL~ suffix to prevent checkin
  view="${template%%.sql}"
  view="${view%%.SQL~}"
  view="${view##*/}"

  echo "Creating "${dst_project}.${dataset}.${view}" using "${template}

  ${BQ_CREATE_VIEW} \
      -src-project "${src_project}" \
      -create-view "${dst_project}.${dataset}.${view}" \
      -template "${template}" \
      -description "${description}" \
      -editor "${USER}"
}

function create_org_joined_view() {
  local project=$1
  local org=$2
  mkdir -p autoload_v2_${org}_ndt
  sed -e 's/{{ORG}}/'${org}'/g' autoload_v2_ndt/ndt7_joined.template.sql > autoload_v2_${org}_ndt/ndt7_joined.sql
  create_view ${project} ${project} autoload_v2_${org}_ndt ./autoload_v2_${org}_ndt/ndt7_joined.sql
}
