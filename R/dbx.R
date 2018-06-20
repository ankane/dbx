#' Create a database connection
#'
#' @param url A database URL
#' @param adapter The database adapter to use
#' @param ... Arguments to pass to dbConnect
#' @importFrom urltools url_parse get_credentials
#' @importFrom DBI dbConnect
#' @export
#' @examples \dontrun{
#'
#' # Postgres
#' db <- dbxConnect(adapter="postgres", dbname="mydb")
#'
#' # MySQL
#' db <- dbxConnect(adapter="mysql", dbname="mydb")
#'
#' # SQLite
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#'
#' # Others
#' db <- dbxConnect(adapter=odbc(), database="mydb")
#' }
dbxConnect <- function(url=NULL, adapter=NULL, ...) {
  if (is.null(adapter) && is.null(url)) {
    url <- Sys.getenv("DATABASE_URL")
  }

  params <- list(...)

  if (!is.null(url)) {
    uri <- url_parse(url)
    creds <- get_credentials(url)
    adapter <- uri$scheme

    params$dbname <- uri$path

    if (!is.na(uri$domain)) {
      params$host <- uri$domain
    }

    if (!is.na(uri$port)) {
      params$port <- uri$port
    }

    if (!is.na(creds$username)) {
      params$user <- creds$username
    }

    if (!is.na(creds$authentication)) {
      params$password <- creds$authentication
    }
  }

  if (!is.character(adapter)) {
    obj <- adapter
  } else if (grepl("postgres", adapter)) {
    if (requireNamespace("RPostgres", quietly=TRUE)) {
      obj <- RPostgres::Postgres()
      if (is.null(params$bigint)) {
        params$bigint <- "numeric"
      }
    } else if (requireNamespace("RPostgreSQL", quietly=TRUE)) {
      obj <- RPostgreSQL::PostgreSQL()
    } else {
      stop("Could not load adapter: RPostgres or RPostgreSQL")
    }
  } else if (grepl("mysql", adapter)) {
    requireLib("RMySQL")
    obj <- RMySQL::MySQL()
  } else if (grepl("sqlite", adapter)) {
    requireLib("RSQLite")
    obj <- RSQLite::SQLite()
  } else {
    stop("Unknown adapter")
  }

  do.call(dbConnect, c(obj, params))
}

#' Close a database connection
#'
#' @param conn A DBIConnection object
#' @importFrom DBI dbDisconnect
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#'
#' dbxDisconnect(db)
dbxDisconnect <- function(conn) {
  dbDisconnect(conn)
}

#' Select records
#'
#' @param conn A DBIConnection object
#' @param statement The SQL statement to use
#' @importFrom DBI dbSendQuery dbFetch dbClearResult
#' @export
#' @examples \dontrun{
#'
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#'
#' records <- dbxSelect(db, "SELECT * FROM forecasts")
#'
#' }
dbxSelect <- function(conn, statement) {
  statement <- processStatement(statement)
  res <- dbSendQuery(conn, statement)
  ret <- dbFetch(res)
  dbClearResult(res)
  ret
}

#' Insert records
#'
#' @param conn A DBIConnection object
#' @param table The table name to insert
#' @param records A data frame of records to insert
#' @param batch_size The number of records to insert in a single statement (defaults to all)
#' @export
#' @examples \dontrun{
#'
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#' records <- data.frame(temperature=c(32, 25))
#'
#' inserts <- dbxInsert(db, table, records)
#' }
dbxInsert <- function(conn, table, records, batch_size=NULL) {
  inBatches(records, batch_size, function(batch) {
    sql <- insertClause(conn, table, batch)
    selectOrExecute(conn, sql, batch)
  })
}

#' Update records
#'
#' @param conn A DBIConnection object
#' @param table The table name to update
#' @param records A data frame of records to insert
#' @param where_cols The columns to use for WHERE clause
#' @param batch_size The number of records to update in a single transaction (defaults to all)
#' @importFrom DBI dbQuoteLiteral dbQuoteIdentifier dbWithTransaction
#' @export
#' @examples \dontrun{
#'
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#' records <- data.frame(id=c(1, 2), temperature=c(16, 13))
#'
#' dbxUpdate(db, table, records, where_cols=c("id"))
#' }
dbxUpdate <- function(conn, table, records, where_cols, batch_size=NULL) {
  cols <- colnames(records)

  if (!identical(intersect(cols, where_cols), where_cols)) {
    stop("where_cols not in records")
  }

  update_cols <- setdiff(cols, where_cols)
  quoted_table <- dbQuoteIdentifier(conn, table)

  inBatches(records, batch_size, function(batch) {
    dbWithTransaction(conn, {
      for (i in 1:nrow(batch)) {
        row <- batch[i,, drop=FALSE]
        sql <- paste("UPDATE", quoted_table, "SET", setClause(conn, row[update_cols]), whereClause(conn, row[where_cols]))
        execute(conn, sql)
      }
    })
  })

  invisible()
}

#' Upsert records
#'
#' @param conn A DBIConnection object
#' @param table The table name to upsert
#' @param records A data frame of records to upsert
#' @param where_cols The columns to use for WHERE clause
#' @param batch_size The number of records to upsert in a single statement (defaults to all)
#' @importFrom DBI dbQuoteLiteral dbQuoteIdentifier dbWithTransaction
#' @export
#' @examples \dontrun{
#'
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#' records <- data.frame(id=c(2, 3), temperature=c(20, 25))
#'
#' upserts <- dbxUpsert(db, table, records, where_cols=c("id"))
#' }
dbxUpsert <- function(conn, table, records, where_cols, batch_size=NULL) {
  cols <- colnames(records)

  if (!identical(intersect(cols, where_cols), where_cols)) {
    stop("where_cols not in records")
  }

  update_cols <- setdiff(cols, where_cols)

  inBatches(records, batch_size, function(batch) {
    if (isMySQL(conn)) {
      sql <- insertClause(conn, table, batch)
      set_sql <- upsertSetClause(conn, update_cols)
      sql <- paste(sql, "ON DUPLICATE KEY UPDATE", set_sql)
      selectOrExecute(conn, sql, batch)
    } else {
      conflict_target <- colsClause(conn, where_cols)
      sql <- insertClause(conn, table, batch)
      set_sql <- upsertSetClausePostgres(conn, update_cols)
      sql <- paste0(sql, " ON CONFLICT (", conflict_target, ") DO UPDATE SET ", set_sql)
      selectOrExecute(conn, sql, batch)
    }
  })
}

#' Delete records
#'
#' @param conn A DBIConnection object
#' @param table The table name to delete records from
#' @param where A data frame of records to delete
#' @param batch_size The number of records to delete in a single statement (defaults to all)
#' @importFrom DBI dbQuoteLiteral dbQuoteIdentifier dbWithTransaction
#' @export
#' @examples \dontrun{
#'
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#'
#' # Delete specific records
#' bad_records <- data.frame(id=c(1, 2))
#' dbxDelete(db, table, where=bad_records)
#'
#' # Delete all records
#' dbxDelete(db, table)
#' }
dbxDelete <- function(conn, table, where=NULL, batch_size=NULL) {
  quoted_table <- dbQuoteIdentifier(conn, table)

  if (is.null(where)) {
    if (isPostgres(conn) || isMySQL(conn)) {
      sql <- paste("TRUNCATE", quoted_table)
    } else {
      sql <- paste("DELETE FROM", quoted_table)
    }
    execute(conn, sql)
  } else if (length(where) == 0) {
    # do nothing
  } else {
    inBatches(where, batch_size, function(batch_where) {
      if (length(batch_where) == 1) {
        quoted_col <- colnames(batch_where)[1]
        sql <- paste("DELETE FROM", quoted_table, "WHERE", quoted_col, "IN", singleValuesRow(conn, where[, 1]))
        execute(conn, sql)
      } else {
        cols <- colnames(batch_where)

        clauses <- c()
        for (i in 1:nrow(batch_where)) {
          row <- batch_where[i,, drop=FALSE]
          clauses <- c(clauses, paste0("(", paste(equalClause(conn, row), collapse=" AND "), ")"))
        }

        sql <- paste("DELETE FROM", quoted_table, "WHERE", paste(clauses, collapse=" OR "))
        execute(conn, sql)
      }
    })
  }

  invisible()
}

equalClause <- function(conn, row) {
  cols <- colnames(row)
  set <- c()
  for (i in cols) {
    set <- c(set, paste(dbQuoteIdentifier(conn, i), "=", dbQuoteLiteral(conn, as.character(row[i][[1]]))))
  }
  set
}

upsertSetClause <- function(conn, cols) {
  paste0(lapply(cols, function(x) {
    col <- dbQuoteIdentifier(conn, as.character(x))
    paste0(col, " = VALUES(", col, ")")
  }), collapse=", ")
}

upsertSetClausePostgres <- function(conn, cols) {
  paste0(lapply(cols, function(x) {
    col <- dbQuoteIdentifier(conn, as.character(x))
    paste0(col, " = excluded.", col)
  }), collapse=", ")
}

colsClause <- function(conn, cols) {
   paste0(lapply(cols, function(y) { dbQuoteIdentifier(conn, y) }), collapse=", ")
}

setClause <- function(conn, row) {
  paste0(equalClause(conn, row), collapse=", ")
}

whereClause <- function(conn, row) {
  paste("WHERE", paste(equalClause(conn, row), collapse=" AND "))
}

singleValuesRow <- function(conn, row) {
  paste0("(", paste0(lapply(row, function(y) { dbQuoteLiteral(conn, as.character(y)) }), collapse=", "), ")")
}

valuesClause <- function(conn, records) {
  paste0(apply(records, 1, function(x) { singleValuesRow(conn, x) }), collapse=", ")
}

insertClause <- function(conn, table, records) {
  cols <- colnames(records)
  quoted_table <- dbQuoteIdentifier(conn, table)
  quoted_cols <- colsClause(conn, cols)
  records_sql <- valuesClause(conn, records)
  paste0("INSERT INTO ", quoted_table, " (", quoted_cols, ") VALUES ", records_sql)
}

requireLib <- function(name) {
  if (!requireNamespace(name, quietly=TRUE)) {
    stop(paste("Could not load adapter:", name))
  }
}

isPostgres <- function(conn) {
  class(conn) == "PostgreSQLConnection" || class(conn) == "PqConnection"
}

isMySQL <- function(conn) {
  class(conn) == "MySQLConnection"
}

selectOrExecute <- function(conn, sql, records) {
  if (isPostgres(conn)) {
    sql <- paste(sql, "RETURNING *")
    ret <- dbxSelect(conn, sql)

    ret_cols <- c()
    for (i in colnames(ret)) {
      if (i %in% colnames(records) || !all(is.na(ret[i]))) {
        ret_cols <- c(ret_cols, i)
      }
    }

    invisible(ret[, ret_cols])
  } else {
    execute(conn, sql)
    invisible(records)
  }
}

#' @importFrom DBI dbExecute
execute <- function(conn, statement) {
  statement <- processStatement(statement)
  dbExecute(conn, statement)
}

processStatement <- function(statement) {
  comment <- getOption("dbx_comment")

  if (!is.null(comment)) {
    if (identical(comment, TRUE)) {
      comment <- paste0("script:", sub(".*=", "", commandArgs()[4]))
    }
    statement <- paste0(statement, " /*", comment, "*/")
  }

  verbose <- getOption("dbx_verbose")
  if (is.function(verbose)) {
    verbose(statement)
  } else if (any(verbose)) {
    message(statement)
  }

  statement
}

inBatches <- function(records, batch_size, f) {
  if (is.null(batch_size)) {
    f(records)
  } else {
    row_count <- nrow(records)
    batch_count <- row_count / batch_size
    ret <- data.frame()
    for(i in 1:batch_count) {
      start <- ((i - 1) * batch_size) + 1
      end <- start + batch_size - 1
      if (end > row_count) {
        end <- row_count
      }
      ret <- rbind(ret, f(records[start:end,, drop=FALSE]))
    }
    ret
  }
}
