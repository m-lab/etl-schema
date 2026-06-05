#!/bin/bash
#
# create_autojoin_dataset_views.sh creates all datasets and views for the
# autojoin pipeline. Like create_dataset_views.sh, every directory is a dataset
# name and every sql file within the dataset subdirectory should be a view query
# template.
#
# Example usage:
#
#  ./create_autojoin_dataset_views.sh "self" mlab-sandbox
#  ./create_autojoin_dataset_views.sh "self" mlab-oti

set -eu
USAGE="$0 <key-name> <source-project>"
KEYNAME=${1:?Please provide a key name to authorize operations or "self"}
SRC_PROJECT=${2:?Please provide source project: $USAGE}

# Setup environment.
BASEDIR=$( realpath $( dirname "${BASH_SOURCE[0]}" ) )
cd ${BASEDIR}

# Initialize library.
source ${BASEDIR}/create_view_lib.sh
create_view_init

echo "Creating autojoin views"
# TODO(soltesz): eliminate this in favor of automation within the autoloader.
# Discover autoload org datasets and their available tables using two queries:
# 1) Project-level SCHEMATA to find all autoload datasets.
# 2) A single UNION ALL across dataset-scoped INFORMATION_SCHEMA.TABLES.
datasets=$( bq query --project_id ${SRC_PROJECT} --nouse_legacy_sql --format=csv \
  "SELECT schema_name FROM \`${SRC_PROJECT}\`.\`region-us\`.INFORMATION_SCHEMA.SCHEMATA
   WHERE schema_name LIKE 'autoload_v2_%_ndt'
     AND schema_name != 'autoload_v2_ndt'
   ORDER BY schema_name" \
  | tail -n +2 )

# Build a single UNION ALL query across all dataset INFORMATION_SCHEMA.TABLES
# to discover which tables exist in each dataset.
tables_query=""
for ds in $datasets ; do
  if [[ -n "$tables_query" ]]; then
    tables_query+=" UNION ALL "
  fi
  tables_query+="SELECT '${ds}' AS table_schema, table_name FROM \`${ds}\`.INFORMATION_SCHEMA.TABLES WHERE table_name IN ('ndt7_raw', 'scamper2_raw')"
done

table_info=""
if [[ -n "$tables_query" ]]; then
  table_info=$( bq query --project_id ${SRC_PROJECT} --nouse_legacy_sql --format=csv \
    "$tables_query" | tail -n +2 )
fi

# Org datasets with ndt7 data.
ndt7_datasets=$( echo "$table_info" | grep ndt7_raw | cut -d, -f1 )
# Org datasets with scamper2 data (not all orgs run traceroute-caller).
scamper2_datasets=$( echo "$table_info" | grep scamper2_raw | cut -d, -f1 )

echo '-- Generated query' > ./autoload_v2_ndt/ndt7_union.sql
for ds in $ndt7_datasets ; do
  org=$( echo $ds | tr '_' ' ' | awk '{print $3}' )
  create_org_joined_view  ${SRC_PROJECT} ${org}
  if grep -q SELECT ./autoload_v2_ndt/ndt7_union.sql ; then
    # If there is already a SELECT statement in the union, append a "UNION ALL" before the next.
    echo 'UNION ALL' >> ./autoload_v2_ndt/ndt7_union.sql
  fi
  echo 'SELECT * FROM `{{.ProjectID}}.'$ds'.ndt7_joined`' >> ./autoload_v2_ndt/ndt7_union.sql
done

# Only deploy view if it contains at least one SELECT.
if grep -q SELECT ./autoload_v2_ndt/ndt7_union.sql ; then
  # NOTE: Must create "ndt7_union" last because it references the views above.
  create_view ${SRC_PROJECT} ${SRC_PROJECT} autoload_v2_ndt ./autoload_v2_ndt/ndt7_union.sql
fi

# scamper2 union across autojoin orgs. Each org's scamper2_raw is joined to its
# annotation2_raw (by connection UUID) to add server & client annotations before
# the union, mirroring the ndt7_joined pattern above.
echo '-- Generated query' > ./autoload_v2_ndt/scamper2_union.sql
for ds in $scamper2_datasets ; do
  org=$( echo $ds | tr '_' ' ' | awk '{print $3}' )
  create_org_scamper2_joined_view ${SRC_PROJECT} ${org}
  if grep -q SELECT ./autoload_v2_ndt/scamper2_union.sql ; then
    # If there is already a SELECT statement in the union, append a "UNION ALL" before the next.
    echo 'UNION ALL BY NAME' >> ./autoload_v2_ndt/scamper2_union.sql
  fi
  echo 'SELECT * FROM `{{.ProjectID}}.'$ds'.scamper2_joined`' >> ./autoload_v2_ndt/scamper2_union.sql
done

if grep -q SELECT ./autoload_v2_ndt/scamper2_union.sql ; then
  create_view ${SRC_PROJECT} ${SRC_PROJECT} autoload_v2_ndt ./autoload_v2_ndt/scamper2_union.sql
fi

echo "All views created successfully"
