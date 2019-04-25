# SQL files for creating views.

This directory contains subdirectories containing .sql files that will be used
to create corresponding views in different projects.

For now, these views should be simple, referencing only a single other table
or view, and the views should NOT be nested very deep - no more than 2 levels
below the dataset's canonical view.

For example, the ndt subdirectory will contain

*  downloads.sql
*  recommended.sql
*  uploads.sql
*  web100.sql

This will result in creation, e.g. for mlab-sandbox, of:

- mlab-sandbox:ndt.web100
- mlab-sandbox:ndt.recommended  - referencing web100
- mlab-sandbox:ndt.downloads - referencing only recommended
- mlab-sandbox:ndt.uploads - referencing only recommended.

Each .sql file will contain a single %s, which will be replaced with 
the appropriate project name.