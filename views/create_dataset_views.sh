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

# Build all views
# Upper level views always have src_project=dst_project=DST_PROJECT
# The bottom level views can access an alternate SRC_PROJECT

# NDT raw (legacy parser)
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/web100_legacy.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/ndt5_legacy.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/tcpinfo_legacy.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/paris1_legacy.sql
# NDT raw - NB: the raw tables are currently in mlab-oti.raw_ndt.
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/annotation2.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/ndt5.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/ndt7.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/pcap.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/hopannotation2.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/scamper1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt_raw ./ndt_raw/tcpinfo.sql

# MSAK raw.
create_view ${SRC_PROJECT} ${DST_PROJECT} msak_raw ./msak_raw/throughput1.sql

# HOST raw.
create_view ${SRC_PROJECT} ${DST_PROJECT} host_raw ./host_raw/nodeinfo1.sql

# WEHE
create_view ${SRC_PROJECT} ${DST_PROJECT} wehe_raw ./wehe_raw/annotation2.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} wehe_raw ./wehe_raw/hopannotation2.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} wehe_raw ./wehe_raw/scamper1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} wehe_raw ./wehe_raw/clientXputs1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} wehe_raw ./wehe_raw/decisions1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} wehe_raw ./wehe_raw/replayInfo1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} wehe ./wehe/clientXputs1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} wehe ./wehe/decisions1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} wehe ./wehe/replayInfo1.sql

# Public pass-through views for joined tables.
if [[ ${DST_PROJECT} = "measurement-lab" ]] ; then
    # NOTE: these steps can only be applied in the public measurement-lab
    # project because in other M-Lab projects, these targets are actual
    # tables. Only in measurement-lab can we create these views.
    create_view ${SRC_PROJECT} ${DST_PROJECT} ndt ./ndt/ndt5.sql
    create_view ${SRC_PROJECT} ${DST_PROJECT} ndt ./ndt/ndt7.sql
    create_view ${SRC_PROJECT} ${DST_PROJECT} ndt ./ndt/tcpinfo.sql
    create_view ${SRC_PROJECT} ${DST_PROJECT} ndt ./ndt/scamper1.sql
    create_view ${SRC_PROJECT} ${DST_PROJECT} ndt ./ndt/web100.sql

    # WEHE
    create_view ${SRC_PROJECT} ${DST_PROJECT} wehe ./wehe/scamper1.sql
    create_view ${SRC_PROJECT} ${DST_PROJECT} wehe ./wehe/scamper1_hopannotation2.sql

    # REVTR
   create_view ${SRC_PROJECT} ${DST_PROJECT} revtr_raw ./revtr_raw/ping1.sql
   create_view ${SRC_PROJECT} ${DST_PROJECT} revtr_raw ./revtr_raw/revtr1.sql
   create_view ${SRC_PROJECT} ${DST_PROJECT} revtr_raw ./revtr_raw/trace1.sql
   create_view ${SRC_PROJECT} ${DST_PROJECT} revtr_raw ./revtr_raw/traceatlas1.sql
fi

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
# Patch to create unified_downloads_nofilter (removes 2 clauses)
sed -e 's/EXCEPT.*//' -e 's/WHERE IsValidBest//' ./ndt/unified_downloads.sql > ./ndt/unified_downloads_nofilter.SQL~
create_view ${DST_PROJECT} ${DST_PROJECT} ndt ./ndt/unified_downloads_nofilter.SQL~
create_view ${DST_PROJECT} ${DST_PROJECT} ndt ./ndt/unified_uploads_20201026x.sql
create_view ${DST_PROJECT} ${DST_PROJECT} ndt ./ndt/unified_uploads.sql
# Patch to create unified_uploads_nofilter (removes 2 clauses)
sed -e 's/EXCEPT.*//' -e 's/WHERE IsValidBest//' ./ndt/unified_uploads.sql > ./ndt/unified_uploads_nofilter.SQL~
create_view ${DST_PROJECT} ${DST_PROJECT} ndt ./ndt/unified_uploads_nofilter.SQL~

# traceroute.
create_view ${SRC_PROJECT} ${DST_PROJECT} traceroute ./traceroute/scamper1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} traceroute ./traceroute/paris1_legacy.sql

# global web100 sidestream (legacy parser)
create_view ${SRC_PROJECT} ${DST_PROJECT} sidestream ./sidestream/web100_legacy.sql

# switch telemetry (legacy parser)
create_view ${SRC_PROJECT} ${DST_PROJECT} utilization ./utilization/switch_legacy.sql

# switch telemetry (v2 parser)
create_view ${SRC_PROJECT} ${DST_PROJECT} utilization ./utilization/switch.sql

# passthrough for mlab-cloudflare tables.
create_view ${SRC_PROJECT} ${DST_PROJECT} cloudflare ./cloudflare/speedtest_speed1.sql

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
