## 0.4.0 (unreleased)

- Added support for DuckDB
- Added support for `returning` option for `dbxInsert` and `dbxUpsert` for SQLite 3.35.0+
- Fixed typecasting with ODBC for MariaDB

## 0.3.2 (2024-06-02)

- Added support for `returning` option for `dbxUpsert` for MariaDB 10.5+

## 0.3.1 (2023-12-11)

- Added support for `dbxUpsert` for SQL Server
- Fixed typecasting with ODBC

## 0.3.0 (2023-11-30)

- Fixed issue with RMariaDB 1.3.0+
- Fixed typecasting for empty results with RPostgreSQL 0.7
- Fixed warning with RPostgreSQL

## 0.2.8 (2021-01-16)

- Added support for `SQL` with `returning` option
- Fixed time zones for RPostgres 1.3.0+ and RMariaDB 1.1.0+

## 0.2.7 (2020-09-07)

- Fixed CRAN check with R-devel

## 0.2.6 (2020-06-14)

- Added `transaction` option to `dbxUpdate`
- Added unsafe version check for RMySQL

## 0.2.5 (2019-04-24)

- Added support for tibbles
- Improved error message for invalid database URL
- Fixed error when `where_cols` ordering different than data frame

## 0.2.4 (2018-12-27)

- Added `dbxExecute` function
- Added support for complex types
- Fixed issues with NULL blobs with Postgres and SQLite

## 0.2.3 (2018-10-30)

- Added parameters for `dbxSelect`
- Added `skip_existing` option to `dbxUpsert`
- Fixed issue with `batch_size` option skipping partial batches

## 0.2.2 (2018-09-05)

- Added statement duration to logging
- Added logging for `dbxUpdate` transaction
- Added `variables` to `dbxConnect`
- Added `connect_timeout`, `sslcert`, `sslkey`, and `sslcrl` for RPostgreSQL
- Prefer `dbx_logging` over `dbx_verbose`
- Improved support for ODBC
- Fixed error with `dbxUpsert`

## 0.2.1 (2018-08-03)

- Added `sslmode` and `sslrootcert` for RPostgreSQL
- Fixed error with dplyr check

## 0.2.0 (2018-07-16)

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

## 0.1.0 (2018-07-05)

- First release
