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
create_org_joined_view  ${SRC_PROJECT} mlab
create_org_joined_view ${SRC_PROJECT} rnp
# NOTE: Must create "ndt7_union" last because it references the views above.
create_view ${SRC_PROJECT} ${SRC_PROJECT} autoload_v2_ndt ./autoload_v2_ndt/ndt7_union.sql

echo "All views created successfully"
