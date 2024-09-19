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
# TODO(soltesz): this should all be automated.
# Get list of orgs with autoloaded data.
datasets=$( bq ls --project_id ${SRC_PROJECT} | grep autoload | grep _ndt | grep -v autoload_v2_ndt )
echo '-- Generated query' > ./autoload_v2_ndt/ndt7_union.sql
for ds in $datasets ; do
  org=$( echo $ds | tr '_' ' ' | awk '{print $3}' )
  create_org_joined_view  ${SRC_PROJECT} ${org}
  if grep -q SELECT ./autoload_v2_ndt/ndt7_union.sql ; then
    echo 'UNION ALL' >> ./autoload_v2_ndt/ndt7_union.sql
  fi
  echo 'SELECT * FROM `{{.ProjectID}}.'$ds'.ndt7_joined`' >> ./autoload_v2_ndt/ndt7_union.sql
done

# NOTE: Must create "ndt7_union" last because it references the views above.
create_view ${SRC_PROJECT} ${SRC_PROJECT} autoload_v2_ndt ./autoload_v2_ndt/ndt7_union.sql

echo "All views created successfully"
