#' Create a database connection
#'
#' @param url A database URL
#' @param adapter The database adapter to use
#' @param ... Arguments to pass to dbConnect
#' @importFrom urltools url_parse get_credentials
#' @importFrom DBI dbConnect
#' @export
#' @examples
#' # SQLite
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#'
#' \dontrun{
#'
#' # Postgres
#' db <- dbxConnect(adapter="postgres", dbname="mydb")
#'
#' # MySQL
#' db <- dbxConnect(adapter="mysql", dbname="mydb")
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
    if (requireNamespace("RMySQL", quietly=TRUE)) {
      obj <- RMySQL::MySQL()
    } else if (requireNamespace("RMariaDB", quietly=TRUE)) {
      obj <- RMariaDB::MariaDB()
      if (is.null(params$bigint)) {
        params$bigint <- "numeric"
      }
    } else {
      stop("Could not load adapter: RMySQL or RMariaDB")
    }
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
#' @importFrom DBI dbSendQuery dbFetch dbClearResult dbHasCompleted
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' DBI::dbCreateTable(db, "forecasts", data.frame(id=1:3, temperature=20:22))
#'
#' records <- dbxSelect(db, "SELECT * FROM forecasts")
dbxSelect <- function(conn, statement) {
  statement <- processStatement(statement)
  res <- dbSendQuery(conn, statement)
  ret <- list()
  while (!dbHasCompleted(res)) {
    ret[[length(ret) + 1]] <- dbFetch(res)
  }
  dbClearResult(res)
  combineResults(ret)
}

#' Insert records
#'
#' @param conn A DBIConnection object
#' @param table The table name to insert
#' @param records A data frame of records to insert
#' @param batch_size The number of records to insert in a single statement (defaults to all)
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#' DBI::dbCreateTable(db, table, data.frame(id=1:3, temperature=20:22))
#'
#' records <- data.frame(temperature=c(32, 25))
#' inserts <- dbxInsert(db, table, records)
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
#' @importFrom DBI dbWithTransaction
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#' DBI::dbCreateTable(db, table, data.frame(id=1:3, temperature=20:22))
#'
#' records <- data.frame(id=c(1, 2), temperature=c(16, 13))
#' dbxUpdate(db, table, records, where_cols=c("id"))
dbxUpdate <- function(conn, table, records, where_cols, batch_size=NULL) {
  cols <- colnames(records)

  if (!identical(intersect(cols, where_cols), where_cols)) {
    stop("where_cols not in records")
  }

  update_cols <- setdiff(cols, where_cols)

  # quote
  quoted_table <- quoteIdent(conn, table)
  quoted_where_cols <- quoteIdent(conn, where_cols)
  quoted_update_cols <- quoteIdent(conn, update_cols)

  inBatches(records, batch_size, function(batch) {
    quoted_records <- quoteRecords(conn, batch)
    colnames(quoted_records) <- colnames(batch)
    groups <- split(quoted_records, quoted_records[update_cols])

    dbWithTransaction(conn, {
      for (group in groups) {
        row <- group[1,, drop=FALSE]
        sql <- paste("UPDATE", quoted_table, "SET", setClause(quoted_update_cols, row[update_cols]), "WHERE", whereClause(quoted_where_cols, group[where_cols]))
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
#' @export
#' @examples \dontrun{
#'
#' db <- dbxConnect(adapter="postgres", dbname="dbx")
#' table <- "forecasts"
#' DBI::dbCreateTable(db, table, data.frame(id=1:3, temperature=20:22))
#'
#' records <- data.frame(id=c(3, 4), temperature=c(20, 25))
#' upserts <- dbxUpsert(db, table, records, where_cols=c("id"))
#' }
dbxUpsert <- function(conn, table, records, where_cols, batch_size=NULL) {
  cols <- colnames(records)

  if (!identical(intersect(cols, where_cols), where_cols)) {
    stop("where_cols not in records")
  }

  update_cols <- setdiff(cols, where_cols)

  # quote
  quoted_where_cols <- quoteIdent(conn, where_cols)
  quoted_update_cols <- quoteIdent(conn, update_cols)

  inBatches(records, batch_size, function(batch) {
    if (isMySQL(conn)) {
      sql <- insertClause(conn, table, batch)
      set_sql <- upsertSetClause(quoted_update_cols)
      sql <- paste(sql, "ON DUPLICATE KEY UPDATE", set_sql)
      selectOrExecute(conn, sql, batch)
    } else {
      conflict_target <- colsClause(quoted_where_cols)
      sql <- insertClause(conn, table, batch)
      set_sql <- upsertSetClausePostgres(quoted_update_cols)
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
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#' DBI::dbCreateTable(db, table, data.frame(id=1:3, temperature=20:22))
#'
#' # Delete specific records
#' bad_records <- data.frame(id=c(1, 2))
#' dbxDelete(db, table, where=bad_records)
#'
#' # Delete all records
#' dbxDelete(db, table)
dbxDelete <- function(conn, table, where=NULL, batch_size=NULL) {
  quoted_table <- quoteIdent(conn, table)

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
    cols <- colnames(where)
    quoted_cols <- quoteIdent(conn, cols)

    inBatches(where, batch_size, function(batch_where) {
      quoted_records <- quoteRecords(conn, batch_where)
      sql <- paste0("DELETE FROM ", quoted_table, " WHERE ", whereClause(quoted_cols, quoted_records))
      execute(conn, sql)
    })
  }

  invisible()
}

equalClause <- function(cols, row) {
  lapply(1:length(cols), function (i) { paste(cols[i] , "=", row[[i]]) })
}

upsertSetClause <- function(cols) {
  paste(lapply(cols, function(x) {
    paste0(x, " = VALUES(", x, ")")
  }), collapse=", ")
}

upsertSetClausePostgres <- function(cols) {
  paste(lapply(cols, function(x) {
    paste0(x, " = excluded.", x)
  }), collapse=", ")
}

colsClause <- function(cols) {
  paste(cols, collapse=", ")
}

setClause <- function(cols, row) {
  paste(equalClause(cols, row), collapse=", ")
}

whereClause <- function(cols, records) {
  if (length(cols) == 1) {
    paste0(cols[1], " IN (", paste(records[, 1], collapse=", ") , ")")
  } else {
    clauses <- apply(records, 1, function(x) { paste0("(", whereClause2(cols, x), ")") })
    paste(clauses, collapse=" OR ")
  }
}

whereClause2 <- function(cols, row) {
  paste(equalClause(cols, row), collapse=" AND ")
}

# could be a faster method than apply
# https://rpubs.com/wch/200398
valuesClause <- function(conn, records) {
  quoted_records <- quoteRecords(conn, records)
  rows <- apply(quoted_records, 1, function(x) { paste0(x, collapse=", ") })
  paste0("(", rows, ")", collapse=", ")
}

insertClause <- function(conn, table, records) {
  cols <- colnames(records)

  # quote
  quoted_table <- quoteIdent(conn, table)
  quoted_cols <- quoteIdent(conn, cols)

  cols_sql <- colsClause(quoted_cols)
  records_sql <- valuesClause(conn, records)
  paste0("INSERT INTO ", quoted_table, " (", cols_sql, ") VALUES ", records_sql)
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
  class(conn) == "MySQLConnection" || class(conn) == "MariaDBConnection"
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
  if (nrow(records) > 0) {
    if (is.null(batch_size)) {
      f(records)
    } else {
      row_count <- nrow(records)
      batch_count <- row_count / batch_size
      ret <- list()
      for(i in 1:batch_count) {
        start <- ((i - 1) * batch_size) + 1
        end <- start + batch_size - 1
        if (end > row_count) {
          end <- row_count
        }
        ret[[length(ret) + 1]] <- f(records[start:end,, drop=FALSE])
      }
      combineResults(ret)
    }
  } else {
    records
  }
}

# https://stackoverflow.com/questions/2851327/convert-a-list-of-data-frames-into-one-data-frame
combineResults <- function(ret) {
  if (isNamespaceLoaded("dplyr")) {
    dplyr::bind_rows(ret)
  } else {
    do.call(rbind, ret)
  }
}

#' @importFrom DBI dbQuoteIdentifier
quoteIdent <- function(conn, cols) {
  as.character(dbQuoteIdentifier(conn, cols))
}

#' @importFrom DBI dbQuoteLiteral
quoteRecords <- function(conn, records) {
  quoted_records <- data.frame(matrix(ncol=0, nrow=nrow(records)))
  for (i in 1:ncol(records)) {
    quoted_records[, i] <- dbQuoteLiteral(conn, records[, i])
  }
  quoted_records
}
