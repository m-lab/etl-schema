# SQL files for creating views.

This directory contains subdirectories containing .sql files that are used
to create corresponding views in different projects.

For now, these views should be simple, referencing only a single other table
or view, and the views should NOT be nested very deep - no more than 2 levels
below the dataset's canonical view.

For example, the ndt subdirectory contains:

*  web100.sql

As well, the ndt subdirectory includes `referenced-by` subdirectories with view
templates that reference the top-level web100 view. For example:

*  referenced-by/recommended.sql

Further `referenced-by` subdirectories may reference the views one level above
it. For example:

*  referenced-by/referenced-by/downloads.sql
*  referenced-by/referenced-by/uploads.sql

This will result in creation, e.g. for mlab-sandbox, of:

- mlab-sandbox:ndt.web100
- mlab-sandbox:ndt.recommended  - referencing web100
- mlab-sandbox:ndt.downloads - referencing only recommended
- mlab-sandbox:ndt.uploads - referencing only recommended.

Each .sql file may contain a single %s, which will be replaced with
the appropriate project name.

## Creating Views

```bash
# To update testing views in sandbox.
./create_dataset_views.sh "self" mlab-sandbox mlab-sandbox

# To update public accessible views.
./create_dataset_views.sh "self" mlab-oti measurement-lab
```
