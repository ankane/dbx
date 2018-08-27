#' @importFrom DBI dbConnect dbDisconnect dbExecute dbQuoteIdentifier dbQuoteLiteral dbSendQuery dbFetch dbClearResult dbHasCompleted dbColumnInfo

isPostgres <- function(conn) {
  isRPostgreSQL(conn) || isRPostgres(conn) || isODBCPostgres(conn)
}

isRPostgreSQL <- function(conn) {
  inherits(conn, "PostgreSQLConnection")
}

isRPostgres <- function(conn) {
  inherits(conn, "PqConnection")
}

isODBCPostgres <- function(conn) {
  inherits(conn, "PostgreSQL")
}

isRMySQL <- function(conn) {
  inherits(conn, "MySQLConnection")
}

isMySQL <- function(conn) {
  isRMySQL(conn) || isRMariaDB(conn) || isODBCMySQL(conn)
}

isRMariaDB <- function(conn) {
  inherits(conn, "MariaDBConnection")
}

isSQLite <- function(conn) {
  inherits(conn, "SQLiteConnection")
}

isODBCMySQL <- function(conn) {
  inherits(conn, "MySQL")
}

isODBC <- function(conn) {
  !is.null(attr(conn, "info")$odbc.version)
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

isDate <- function(col) {
  inherits(col, "Date")
}

isDatetime <- function(col) {
  inherits(col, "POSIXt")
}

isTime <- function(col) {
  inherits(col, "hms")
}

isLogical <- function(col) {
  inherits(col, "logical")
}

isBinary <- function(col) {
  is.raw(col[[1]])
}

isBlob <- function(col) {
  inherits(col, "blob")
}

selectOrExecute <- function(conn, sql, records, returning) {
  if (is.null(returning)) {
    execute(conn, sql)
    invisible()
  } else {
    if (!isPostgres(conn)) {
      stop("returning is only supported with Postgres")
    }

    returning_clause = paste(lapply(returning, function(x) { if (x == "*") x else quoteIdent(conn, x) }), collapse=", ")
    sql <- paste(sql, "RETURNING", returning_clause)

    dbxSelect(conn, sql)
  }
}

execute <- function(conn, statement) {
  statement <- processStatement(statement)
  timeStatement(statement, {
    dbExecute(conn, statement)
  })
}

processStatement <- function(statement) {
  comment <- getOption("dbx_comment")

  if (!is.null(comment)) {
    if (isTRUE(comment)) {
      comment <- paste0("script:", scriptName())
    }
    statement <- paste0(statement, " /*", comment, "*/")
  }

  statement
}

scriptName <- function() {
  sub(".*=", "", commandArgs()[4])
}

timeStatement <- function(statement, code) {
  started_at <- Sys.time()

  tryCatch(code, finally={
    duration <- round(as.double(difftime(Sys.time(), started_at)) * 1000, 1)

    verbose <- getOption("dbx_logging", getOption("dbx_verbose"))
    if (is.function(verbose)) {
      verbose(statement)
    } else if (any(verbose)) {
      message(paste0("(", duration, "ms) ", statement))
    }
  })
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
  if (length(ret) == 1) {
    ret[[1]]
  } else {
    do.call(rbind, ret)
  }
}

storageTimeZone <- function(conn) {
  tz <- attr(conn, "dbx_storage_tz")
  if (is.null(tz)) "Etc/UTC" else tz
}

currentTimeZone <- function() {
  Sys.getenv("TZ", Sys.timezone())
}

quoteIdent <- function(conn, cols) {
  as.character(dbQuoteIdentifier(conn, cols))
}

quoteRecords <- function(conn, records) {
  quoted_records <- data.frame(matrix(ncol=0, nrow=nrow(records)))
  for (i in 1:ncol(records)) {
    col <- records[, i]
    if (isMySQL(conn)) {
      if (isDatetime(col)) {
        col <- format(col, tz=storageTimeZone(conn), "%Y-%m-%d %H:%M:%OS6")
      } else if (isDate(col)) {
        col <- format(col)
      } else if (isTime(col)) {
        col <- format(col)
      }
    } else if (isPostgres(conn)) {
      if (isDatetime(col)) {
        col <- format(col, tz=storageTimeZone(conn), "%Y-%m-%d %H:%M:%OS6 %Z")
      } else if (isTime(col)) {
        col <- format(col)
      } else if (isLogical(col) && (isRPostgreSQL(conn) || isODBCPostgres(conn))) {
        col <- as.character(col)
      } else if (isBinary(col)) {
        if (isRPostgreSQL(conn)) {
          col <- as.character(lapply(col, function(x) { RPostgreSQL::postgresqlEscapeBytea(conn, x) }))
        } else {
          # removes AsIs
          col <- blob::as.blob(lapply(col, function(x) { x }))
        }
      }
    } else if (isSQLite(conn)) {
      # since no standard, store dates and datetimes in the same format as Rails
      # store times without dates as strings to keep things simple
      if (isDatetime(col)) {
        col <- format(col, tz=storageTimeZone(conn), "%Y-%m-%d %H:%M:%OS6")
      } else if (isDate(col)) {
        col <- format(col)
      } else if (isTime(col)) {
        col <- format(col)
      }
    }
    quoted_records[, i] <- dbQuoteLiteral(conn, col)
  }
  quoted_records
}
