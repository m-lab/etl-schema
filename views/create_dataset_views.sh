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

# Setup environment.
BASEDIR=$( realpath $( dirname "${BASH_SOURCE[0]}" ) )
cd ${BASEDIR}

# Initialize library.
source ${BASEDIR}/create_view_lib.sh
create_view_init

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
create_view ${SRC_PROJECT} ${DST_PROJECT} msak_raw ./msak_raw/latency1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} msak_raw ./msak_raw/annotation2.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} msak_raw ./msak_raw/hopannotation2.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} msak_raw ./msak_raw/pcap.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} msak_raw ./msak_raw/scamper1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} msak_raw ./msak_raw/tcpinfo.sql

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
   create_view ${SRC_PROJECT} ${DST_PROJECT} revtr_raw ./revtr_raw/ranked_spoofers1.sql
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

# union across autojoin orgs
create_view ${SRC_PROJECT} ${DST_PROJECT} autojoin_autoload_v2_ndt ./autojoin_autoload_v2_ndt/ndt7_union.sql
# union between legacy and autojoin.  These create new names
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt ./ndt/ndt7_legacy.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt ./ndt/ndt7_autojoin.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} ndt ./ndt/ndt7_union.sql

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

# MSAK
create_view ${SRC_PROJECT} ${DST_PROJECT} msak ./msak/throughput1.sql
create_view ${SRC_PROJECT} ${DST_PROJECT} msak ./msak/throughput1_downloads.sql

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
