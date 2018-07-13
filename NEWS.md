# dbx 0.2.0 [unreleased]

- Added support for fractional seconds
- Fixed errors with dates and times
- Fixed date and time typecasting with RMySQL
- Fixed boolean writes with RPostgreSQL

Breaking

- `timestamp without time zone` columns in Postgres are now stored in UTC instead of local time. This does not affect `timestamp with time zone` columns. To keep the previous behavior, use:

  ```r
  dbxConnect(adapter="postgres", storage_tz=Sys.timezone(), ...)
  ```

# dbx 0.1.0

- First release
