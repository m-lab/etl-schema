# SQL files for creating views.

This directory contains subdirectories containing .sql files that are used
to create corresponding views in different projects.

For now, these views should be simple, referencing only a single other table
or view.

For example, the ndt subdirectory contains:

* ndt7.sql

This will result in creation, e.g. in mlab-sandbox, of:

* mlab-sandbox:ndt.ndt7

Each .sql file may contain a Go text/template referencing fields of a
`bigquery.Table`, e.g. `{{.ProjectID}}` which will be replaced with
the current project name.

## Creating Views

```bash
# To update testing views in sandbox.
./create_dataset_views.sh "self" mlab-sandbox mlab-sandbox

# To update public accessible views.
./create_dataset_views.sh "self" mlab-oti measurement-lab
```

## Cloud Build Permissions

View deployment is typically managed by Cloud Build. When views in one project
reference tables in a second project, the Cloud Build service account must have
permission to:

* create views in the first project
* modify the target datasets of the second project (to add the first project to
  "Authorized views" in the second)

Management of these permissions remains manual.

### Deployment to measurement-lab

Members of the [discuss@measurementlab.net][discuss] mailing list may query all
M-Lab data BigQuery data free of charge.

All M-Lab data is accessed through views published in the `measurement-lab` GCP
Project. Not all views in this project are visible to the public by default. To
make a `measurement-lab` dataset and views visible to the public, we must assign
the disuss@measurementlab.net user specific roles.

[discuss]: https://groups.google.com/a/measurementlab.net/g/discuss?pli=1

* `public-bigquery-user-dataset-level`

  This role provides discuss@measurementlab.net group users with access to
  specific datasets in the measurement-lab project. By default datasets are not
  visible to discuss@ users. Apply this role to individual BQ datasets to make
  them publicly viewable.

    ```txt
    bigquery.datasets.get
    bigquery.datasets.getIamPolicy
    bigquery.jobs.create
    bigquery.jobs.list
    bigquery.readsessions.create
    bigquery.readsessions.getData
    bigquery.readsessions.update
    bigquery.savedqueries.get
    bigquery.savedqueries.list
    bigquery.tables.export
    bigquery.tables.get
    bigquery.tables.getData
    bigquery.tables.list
    resourcemanager.projects.get
    ```

* `public-bigquery-user-project-level`

  This role provides discuss@measurementlab.net group users with access to
  tables and views, ability to select measurement-lab project, create jobs
  within the web console & SDK, to save personal queries, and to export data.
  This role is assigned to the discuss@ user in the project IAM settings, and
  automatically inhereited by all BigQuery tables (no production deployment step
  necessary).

  Role permissions include:

    ```txt
    bigquery.jobs.create
    bigquery.jobs.list
    bigquery.savedqueries.get
    bigquery.savedqueries.list
    bigquery.tables.export
    bigquery.tables.getData
    resourcemanager.projects.get
    bigquery.readsessions.create
    bigquery.readsessions.getData
    ```
