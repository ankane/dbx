# dbx

:fire: A fast, easy-to-use database library for R

- Intuitive functions
- High performance batch operations
- Safe inserts, updates, and deletes without writing SQL
- Upserts!!
- Great date and time support
- Works well with auto-incrementing primary keys
- Built on top of [DBI](https://cran.r-project.org/package=DBI)

Designed for both research and production environments

Supports Postgres, MySQL, SQLite, and more

![Screenshot](https://gist.github.com/ankane/b6988db2802aca68a589b31e41b44195/raw/619e19addf388c14e905ed475121fe5806bf8991/dbx.png)

[![Build Status](https://travis-ci.org/ankane/dbx.svg?branch=master)](https://travis-ci.org/ankane/dbx) [![CRAN status](https://www.r-pkg.org/badges/version/dbx)](https://cran.r-project.org/package=dbx)

## Installation

Install dbx

```r
install.packages("dbx")
```

And follow the instructions for your database

- [Postgres](#postgres)
- [MySQL](#mysql)
- [SQLite](#sqlite)
- [SQL Server](#sql-server)
- [Redshift](#redshift)
- [Others](#others)

To install with [Jetpack](https://github.com/ankane/jetpack), use:

```r
jetpack::add("dbx")
```

### Postgres

Install the R package

```r
install.packages("RPostgres")
```

And use:

```r
library(dbx)

db <- dbxConnect(adapter="postgres", dbname="mydb")
```

You can also pass `user`, `password`, `host`, `port`, and `url`.

> Works with RPostgreSQL as well

### MySQL

Install the R package

```r
install.packages("RMySQL")
```

And use:

```r
library(dbx)

db <- dbxConnect(adapter="mysql", dbname="mydb")
```

You can also pass `user`, `password`, `host`, `port`, and `url`.

> Works with RMariaDB as well

### SQLite

Install the R package

```r
install.packages("RSQLite")
```

And use:

```r
library(dbx)

db <- dbxConnect(adapter="sqlite", dbname=":memory:")
```

### SQL Server

Install the R package

```r
install.packages("odbc")
```

And use:

```r
library(dbx)

db <- dbxConnect(adapter=odbc::odbc(), database="mydb")
```

You can also pass `uid`, `pwd`, `server`, and `port`.

### Redshift

For Redshift, follow the [Postgres instructions](#postgres).

### Others

Install the appropriate R package and use:

```r
db <- dbxConnect(adapter=odbc::odbc(), database="mydb")
```

## Operations

### Select

Create a data frame of records from a SQL query

```r
records <- dbxSelect(db, "SELECT * FROM forecasts")
```

Pass parameters

```r
dbxSelect(db, "SELECT * FROM forecasts WHERE period = ? AND temperature > ?", params=list("hour", 27))
```

Parameters can also be vectors

```r
dbxSelect(db, "SELECT * FROM forecasts WHERE id IN (?)", params=list(1:3))
```

### Insert

Insert records

```r
table <- "forecasts"
records <- data.frame(temperature=c(32, 25))
dbxInsert(db, table, records)
```

If you use auto-incrementing ids in Postgres, you can get the ids of newly inserted rows by passing the column name:

```r
dbxInsert(db, table, records, returning=c("id"))
```

### Update

Update records

```r
records <- data.frame(id=c(1, 2), temperature=c(16, 13))
dbxUpdate(db, table, records, where_cols=c("id"))
```

Use `where_cols` to specify the columns used for lookup. Other columns are written to the table.

> Updates are batched when possible, but often need to be run as multiple queries. We recommend upsert when possible for better performance, as it can always be run as a single query. Turn on logging to see the difference.

### Upsert

*Atomically* insert if they don’t exist, otherwise update them

```r
records <- data.frame(id=c(2, 3), temperature=c(20, 25))
dbxUpsert(db, table, records, where_cols=c("id"))
```

Use `where_cols` to specify the columns used for lookup. There must be a unique index on them, or an error will be thrown.

*Only available for PostgreSQL 9.5+, MySQL 5.5+, and SQLite 3.24+*

To skip existing rows instead of updating them, use:

```r
dbxUpsert(db, table, records, where_cols=c("id"), skip_existing=TRUE)
```

If you use auto-incrementing ids in Postgres, you can get the ids of newly upserted rows by passing the column name:

```r
dbxUpsert(db, table, records, where_cols=c("id"), returning=c("id"))
```

### Delete

Delete specific records

```r
bad_records <- data.frame(id=c(1, 2))
dbxDelete(db, table, where=bad_records)
```

Delete all records (uses `TRUNCATE` when possible for performance)

```r
dbxDelete(db, table)
```

### Execute [master]

Execute a statement

```r
dbxExecute(db, "UPDATE forecasts SET temperature = temperature + 1")
```

Pass parameters

```r
dbxExecute(db, "UPDATE forecasts SET temperature = ? WHERE id IN (?)", params=list(27, 1:3))
```

## Logging

Log all SQL queries with:

```r
options(dbx_logging=TRUE)
```

Customize logging by passing a function

```r
logQuery <- function(sql) {
  # your logging code
}

options(dbx_logging=logQuery)
```

## Database Credentials

Environment variables are a convenient way to store database credentials. This keeps them outside your source control. It’s also how platforms like [Heroku](https://www.heroku.com) store them.

Create an `.Renviron` file in your home directory with:

```
DATABASE_URL=postgres://user:pass@host/dbname
```

Install [urltools](https://cran.r-project.org/package=urltools):

```r
install.packages("urltools")
```

And use:

```r
db <- dbxConnect()
```

If you have multiple databases, use a different variable name, and:

```r
db <- dbxConnect(url=Sys.getenv("OTHER_DATABASE_URL"))
```

## Batching

By default, operations are performed in a single statement or transaction. This is better for performance and prevents partial writes on failures. However, when working with large data frames on production systems, it can be better to break writes into batches. Use the `batch_size` option to do this.

```r
dbxInsert(db, table, records, batch_size=1000)
dbxUpdate(db, table, records, where_cols, batch_size=1000)
dbxUpsert(db, table, records, where_cols, batch_size=1000)
dbxDelete(db, table, records, where, batch_size=1000)
```

## Query Comments

Add comments to queries to make it easier to see where time-consuming queries are coming from.

```r
options(dbx_comment=TRUE)
```

The comment will be appended to queries, like:

```sql
SELECT * FROM users /*script:forecast.R*/
```

Set a custom comment with:

```r
options(dbx_comment="hi")
```

## Transactions

To perform multiple operations in a single transaction, use:

```r
DBI::dbWithTransaction(db, {
  dbxInsert(db, ...)
  dbxDelete(db, ...)
})
```

## Data Type Notes

### Dates & Times

Dates are returned as `Date` objects and times as `POSIXct` objects. Times are stored in the database in UTC and converted to your local time zone when retrieved.

Times without dates are returned as `character` vectors since R has no built-in support for this type. If you use [hms](https://cran.r-project.org/package=hms), you can convert columns with:

```r
records$column <- hms::as.hms(records$column)
```

SQLite does not have support for `TIME` columns, so we recommend storing as `VARCHAR`.

### JSON

JSON and JSONB columns are returned as `character` vectors. You can use [jsonlite](https://cran.r-project.org/package=jsonlite) to parse them with:

```r
records$column <- lapply(records$column, jsonlite::fromJSON)
```

SQLite does not have support for `JSON` columns, so we recommend storing as `TEXT`.

### Binary Data

BLOB and BYTEA columns are returned as `raw` vectors.

## Data Type Limitations

### Dates & Times

RSQLite does not currently provide enough info to automatically typecast dates and times. You can manually typecast date columns with:

```r
records$column <- as.Date(records$column)
```

And time columns with:

```r
records$column <- as.POSIXct(records$column, tz="Etc/UTC")
attr(records$column, "tzone") <- Sys.timezone()
```

### Booleans

RMariaDB and RSQLite do not currently provide enough info to automatically typecast booleans. You can manually typecast with:

```r
records$column <- records$column != 0
```

### JSON

RMariaDB does [not currently support JSON](https://github.com/r-dbi/DBI/issues/203).

### Binary Data

RMySQL can write BLOB columns, but [can’t retrieve them directly](https://github.com/r-dbi/RMySQL/issues/123). To workaround this, use:

```r
records <- dbxSelect(db, "SELECT HEX(column) AS column FROM table")

hexToRaw <- function(x) {
  y <- strsplit(x, "")[[1]]
  z <- paste0(y[c(TRUE, FALSE)], y[c(FALSE, TRUE)])
  as.raw(as.hexmode(z))
}

records$column <- lapply(records$column, hexToRaw)
```

## Connection Pooling

Install the [pool](https://cran.r-project.org/package=pool) package

```r
install.packages("pool")
```

Create a pool

```r
library(pool)

factory <- function() {
  dbxConnect(adapter="postgres", ...)
}

pool <- poolCreate(factory, maxSize=5)
```

Run queries

```ruby
conn <- poolCheckout(pool)

tryCatch({
  dbxSelect(conn, "SELECT * FROM forecasts")
}, finally={
  poolReturn(conn)
})
```

In the future, dbx commands may work directly with pools.

## Security

When connecting to a database over a network you don’t fully trust, make sure your [connection is secure](https://ankane.org/postgres-sslmode-explained).

With Postgres, use:

```r
db <- dbxConnect(adapter="postgres", sslmode="verify-full", sslrootcert="ca.pem")
```

With RMariaDB, use:

```r
db <- dbxConnect(adapter="mysql", ssl.ca="ca.pem")
```

Please [let us know](https://github.com/ankane/dbx/issues/new) if you have a way that works with RMySQL.

## Variables

Set session variables with:

```r
db <- dbxConnect(variables=list(search_path="archive"))
```

## Timeouts

Set a statement timeout with:

```r
# Postgres
db <- dbxConnect(variables=list(statement_timeout=1000)) # ms

# MySQL 5.7.8+
db <- dbxConnect(variables=list(max_execution_time=1000)) # ms

# MariaDB 10.1.1+
db <- dbxConnect(variables=list(max_statement_time=1)) # sec
```

With Postgres, set a connect timeout with:

```r
db <- dbxConnect(connect_timeout=3) # sec
```

## Reference

To close a connection, use:

```r
dbxDisconnect(db)
```

All connections are simply [DBI](https://cran.r-project.org/package=DBI) connections, so you can use them with DBI functions as well.

```r
dbGetInfo(db)
```

## Upgrading

### 0.2.0

Version 0.2.0 brings a number of fixes and improvements to data types.

However, there a few breaking changes to be aware of:

- The `dbxInsert` and `dbxUpsert` functions no longer return a data frame by default. For MySQL and SQLite, the data frame was just the `records` argument. For Postgres, if you use auto-incrementing primary keys, the data frame contained ids of the newly inserted/upserted records. To get the ids, pass name of the column as the `returning` argument:

  ```r
  dbxInsert(db, table, records, returning=c("id"))
  ```

- `timestamp without time zone` columns in Postgres are now stored in UTC instead of local time by default. This does not affect `timestamp with time zone` columns. To keep the previous behavior, use:

  ```r
  dbxConnect(adapter="postgres", storage_tz=Sys.timezone(), ...)
  ```

## History

View the [changelog](https://github.com/ankane/dbx/blob/master/NEWS.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/dbx/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/dbx/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development and testing:

```sh
git clone https://github.com/ankane/dbx.git
cd dbx

# create Postgres database
createdb dbx_test

# create MySQL database
mysql -u root -e "CREATE DATABASE dbx_test"
```

In R, do:

```r
install.packages("devtools")
devtools::install_deps(dependencies=TRUE)
devtools::test()
```
