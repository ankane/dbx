# dbx 0.2.5

- Added support for tibbles
- Improved error message for invalid database URL
- Fixed error when `where_cols` ordering different than data frame

# dbx 0.2.4

- Added `dbxExecute` function
- Added support for complex types
- Fixed issues with NULL blobs with Postgres and SQLite

# dbx 0.2.3

- Added parameters for `dbxSelect`
- Added `skip_existing` option to `dbxUpsert`
- Fixed issue with `batch_size` option skipping partial batches

# dbx 0.2.2

- Added statement duration to logging
- Added logging for `dbxUpdate` transaction
- Added `variables` to `dbxConnect`
- Added `connect_timeout`, `sslcert`, `sslkey`, and `sslcrl` for RPostgreSQL
- Prefer `dbx_logging` over `dbx_verbose`
- Improved support for ODBC
- Fixed error with `dbxUpsert`

# dbx 0.2.1

- Added `sslmode` and `sslrootcert` for RPostgreSQL
- Fixed error with dplyr check

# dbx 0.2.0

- Added support for fractional seconds
- Fixed error with updates
- Fixed times for RSQLite
- Fixed typecasting for dates, times, and booleans with RMySQL
- Fixed error when writing binary objects with RPostgres and RPostgreSQL
- Fixed error when writing dates with RMySQL and RMariaDB
- Fixed error when writing booleans with RPostgreSQL
- Fixed error when writing `hms` objects

Breaking

- The `dbxInsert` and `dbxUpsert` functions no longer return a data frame by default. For MySQL and SQLite, the data frame was just the `records` argument. For Postgres, if you use auto-incrementing primary keys, the data frame contained ids of the newly inserted/upserted records. To get the ids, pass name of the column as the `returning` argument:

  ```r
  dbxInsert(db, table, records, returning=c("id"))
  ```

- `timestamp without time zone` columns in Postgres are now stored in UTC instead of local time by default. This does not affect `timestamp with time zone` columns. To keep the previous behavior, use:

  ```r
  dbxConnect(adapter="postgres", storage_tz=Sys.timezone(), ...)
  ```

# dbx 0.1.0

- First release
