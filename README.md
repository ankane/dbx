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

[![Build Status](https://travis-ci.org/ankane/dbx.svg?branch=master)](https://travis-ci.org/ankane/dbx)

## Installation

Install dbx

```r
install.packages("devtools")
devtools::install_github("ankane/dbx")
```

And follow the instructions for your database library

- [Postgres](#postgres)
- [MySQL](#mysql)
- [SQLite](#sqlite)
- [Others](#others)

To install with [Jetpack](https://github.com/ankane/jetpack), use:

```r
jetpack::add("dbx", remote="ankane/dbx")
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

### Others

Install the appropriate R package and use:

```r
db <- dbxConnect(adapter=odbc(), database="mydb")
```

## Operations

### Select

Create a data frame of records from a SQL query

```r
records <- dbxSelect(db, "SELECT * FROM forecasts")
```

### Insert

Insert records

```r
table <- "forecasts"
records <- data.frame(temperature=c(32, 25))
dbxInsert(db, table, records)
```

If you use auto-incrementing ids in Postgres, the ids of the newly inserted rows can be returned by passing the column name:

```r
ids <- dbxInsert(db, table, records, returning=c("id"))
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

If you use auto-incrementing ids in Postgres, the ids of the newly inserted rows can be returned by passing the column name:

```r
ids <- dbxUpsert(db, table, records, where_cols=c("id"), returning=c("id"))
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

## Logging

Log all SQL queries with:

```r
options(dbx_verbose=TRUE)
```

Customize logging by passing a function

```r
logQuery <- function(sql) {
  # your logging code
}

options(dbx_verbose=logQuery)
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

Times without dates are returned as `character` vectors since R has no built-in support for this type. If you use [hms](https://cran.r-project.org/package=hms), can you convert columns with:

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

RSQLite does [not provide enough info](https://github.com/r-dbi/RSQLite/issues/263) to typecast automatically right now. You can manually typecast date columns with:

```r
records$column <- as.Date(records$column)
```

And time columns with:

```r
records$column <- as.POSIXct(records$column, tz="Etc/UTC")
attr(records$column, "tzone") <- Sys.timezone()
```

### Booleans

RMariaDB and RSQLite do [not provide enough info](https://github.com/r-dbi/RMariaDB/issues/100) to typecast automatically right now. You can manually typecast with:

```r
records$column <- records$column != 0
```

### JSON

RMariaDB does [not currently support JSON](https://github.com/r-dbi/DBI/issues/203).

### Binary Data

RMySQL can write BLOB columns, but [can’t retrieve them directly](https://github.com/r-dbi/RMySQL/issues/123). As a workaround, you can do:

```r
records <- dbxSelect(db, "SELECT HEX(column) AS column FROM table")

hexToRaw <- function(x) {
  y <- strsplit(x, "")[[1]]
  z <- paste0(y[c(TRUE, FALSE)], y[c(FALSE, TRUE)])
  as.raw(as.hexmode(z))
}

records$column <- lapply(records$column, hexToRaw)
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

### 0.2.0 [unreleased]

Version 0.2.0 brings a number of improvements to dates and times.

One breaking change is `timestamp without time zone` columns in Postgres are now stored in UTC instead of local time. To keep the previous behavior, use:

```r
dbxConnect(adapter="postgres", storage_tz=Sys.timezone(), ...)
```

This does not affect `timestamp with time zone` columns.

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
