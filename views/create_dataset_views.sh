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

BASEDIR=$( realpath $( dirname "${BASH_SOURCE[0]}" ) )
cd ${BASEDIR}

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
if [[ -x ${BASEDIR}/bq_create_view ]] ; then
  BQ_CREATE_VIEW=${BASEDIR}/bq_create_view
fi

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
  view="${template%%.sql}"
  view="${view##*/}"

  echo -n "Creating "${dst_project}.${dataset}.${view}" using "${template}

  ${BQ_CREATE_VIEW} \
      -src-project "${src_project}" \
      -create-view "${dst_project}.${dataset}.${view}" \
      -template "${template}" \
      -description "${description}" \
      -editor "${USER}"
}

# Build all views
# Upper level views always have src_project=dst_project=DST_PROJECT
# The bottom level views can access an alternate SRC_PROJECT

# NDT raw (legacy parser)
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/web100_legacy.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/ndt5_legacy.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/tcpinfo_legacy.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/traceroute_legacy.sql
# NDT raw - NB: the raw tables are currently in mlab-oti.raw_ndt.
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/annotation.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/ndt7.sql

# NDT extended (mixed parsers)
create_view ${DST_PROJECT} ${DST_PROJECT} ndt_intermediate ./ndt_intermediate/extended_ndt5_downloads.sql
create_view ${DST_PROJECT} ${DST_PROJECT} ndt_intermediate ./ndt_intermediate/extended_ndt5_uploads.sql
create_view ${DST_PROJECT} ${DST_PROJECT} ndt_intermediate ./ndt_intermediate/extended_ndt7_downloads.sql
create_view ${DST_PROJECT} ${DST_PROJECT} ndt_intermediate ./ndt_intermediate/extended_ndt7_uploads.sql
create_view ${DST_PROJECT} ${DST_PROJECT} ndt_intermediate ./ndt_intermediate/extended_web100_downloads.sql
create_view ${DST_PROJECT} ${DST_PROJECT} ndt_intermediate ./ndt_intermediate/extended_web100_uploads.sql

# NDT Unified
create_view ${DST_PROJECT} ${DST_PROJECT} ndt ./ndt/unified_downloads_20201026x.sql
create_view ${DST_PROJECT} ${DST_PROJECT} ndt ./ndt/unified_downloads.sql
create_view ${DST_PROJECT} ${DST_PROJECT} ndt ./ndt/unified_uploads_20201026x.sql
create_view ${DST_PROJECT} ${DST_PROJECT} ndt ./ndt/unified_uploads.sql

# traceroute (legacy parser)
create_view ${SRC_PROJECT} ${DST_PROJECT} aggregate ./aggregate/traceroute.sql

# global web100 sidestream (legacy parser)
create_view ${SRC_PROJECT} ${DST_PROJECT} sidestream ./sidestream/web100.sql

# switch telemetry (legacy parser)
create_view ${SRC_PROJECT} ${DST_PROJECT} utilization ./utilization/switch.sql

# website examples
create_view ${DST_PROJECT} ${DST_PROJECT} website ./website/entry07_platform_decile_downloads_dedup_daily_after.sql
create_view ${DST_PROJECT} ${DST_PROJECT} website ./website/entry07_platform_decile_downloads_dedup_daily_before.sql
create_view ${DST_PROJECT} ${DST_PROJECT} website ./website/entry07_platform_decile_uploads_dedup_daily_after.sql
create_view ${DST_PROJECT} ${DST_PROJECT} website ./website/entry07_platform_decile_uploads_dedup_daily_before.sql
create_view ${DST_PROJECT} ${DST_PROJECT} website ./website/entry07_platform_hourly_downloads_after.sql
create_view ${DST_PROJECT} ${DST_PROJECT} website ./website/entry07_platform_hourly_downloads_before.sql
create_view ${DST_PROJECT} ${DST_PROJECT} website ./website/entry07_platform_hourly_uploads_after.sql
create_view ${DST_PROJECT} ${DST_PROJECT} website ./website/entry07_platform_hourly_uploads_before.sql

# stats-pipeline
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_global_asn.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_continents.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_continents_asn.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_countries.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_countries_asn.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_regions.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_regions_asn.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_cities.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_cities_asn.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_us_states.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_us_states_asn.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_us_counties.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} statistics ./statistics/v0_us_counties_asn.sql

echo "All views created successfully"
