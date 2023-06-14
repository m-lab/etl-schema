# Table Functions

Like the `views` and `transform` directories, the `functions` directory contains definitions
and deployment automation for BigQuery table functions.

## Add a New Table Function

Create an SQL file with the dataset and function definition. The function name
must not collide with an existing table or view name.

The SQL file should define the dataset name and table function. To prevent
confusion, the SQL file name should include the intended dataset and table
function names. For example:

* If the target dataset and table name is `ops.ndt7_download_pdf`, then the
  configuration file should be named `ops.ndt7_download_pdf.sql`.

## Deployment

The deployment script finds all SQL files and applies them one at a time.

```sh
./create_table_functions.sh $PROJECT
```
