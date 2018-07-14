#' Create a database connection
#'
#' @param url A database URL
#' @param adapter The database adapter to use
#' @param storage_tz The time zone timestamps are stored in
#' @param cast_json Cast json columns
#' @param cast_times Cast time columns to 'hms' objects
#' @param cast_blobs Cast blob columns to 'blob' objects
#' @param ... Arguments to pass to dbConnect
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
dbxConnect <- function(url=NULL, adapter=NULL, storage_tz=NULL, cast_json=FALSE, cast_times=NULL, cast_blobs=NULL, ...) {
  if (is.null(adapter) && is.null(url)) {
    url <- Sys.getenv("DATABASE_URL")
  }

  params <- list(...)

  if (!is.null(url)) {
    if (!requireNamespace("urltools", quietly=TRUE)) {
      stop("Install 'urltools' to use url")
    }

    uri <- urltools::url_parse(url)
    creds <- urltools::get_credentials(url)
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

  obj <- findAdapter(adapter)

  if (is.null(obj)) {
    # if not found by exact name

    if (grepl("postgres", adapter)) {
      if (requireNamespace("RPostgres", quietly=TRUE)) {
        adapter <- "rpostgres"
      } else if (requireNamespace("RPostgreSQL", quietly=TRUE)) {
        adapter <- "rpostgresql"
      } else {
        stop("Could not load adapter: RPostgres or RPostgreSQL")
      }
    } else if (grepl("mysql", adapter)) {
      if (requireNamespace("RMySQL", quietly=TRUE)) {
        adapter <- "rmysql"
      } else if (requireNamespace("RMariaDB", quietly=TRUE)) {
        adapter <- "rmariadb"
      } else {
        stop("Could not load adapter: RMySQL or RMariaDB")
      }
    } else if (grepl("sqlite", adapter)) {
      adapter <- "rsqlite"
    } else {
      stop("Unknown adapter")
    }

    obj <- findAdapter(adapter)
  }

  if (is.null(params$bigint) && (inherits(obj, "PqDriver") || inherits(obj, "MariaDBDriver"))) {
    params$bigint <- "numeric"
  }

  conn <- do.call(dbConnect, c(obj, params))

  if (!is.null(storage_tz)) {
    if (!isPostgres(conn)) {
      dbDisconnect(conn)
      stop("storage_tz is only supported with Postgres")
    }
    attr(conn, "dbx_storage_tz") <- storage_tz
  }

  if (cast_json) {
    if (!requireNamespace("jsonlite", quietly=TRUE)) {
      stop("'jsonlite' package is required for cast_json")
    }
    attr(conn, "dbx_cast_json") <- cast_json
  }

  if (!is.null(cast_times)) {
    if (cast_times && !requireNamespace("hms", quietly=TRUE)) {
      stop("'hms' package is required for cast_times")
    }
    attr(conn, "dbx_cast_times") <- cast_times
  }

  if (!is.null(cast_blobs)) {
    if (cast_blobs && !requireNamespace("blob", quietly=TRUE)) {
      stop("'blob' package is required for cast_blobs")
    }
    attr(conn, "dbx_cast_blobs") <- cast_blobs
  }

  # other adapters do this automatically
  if (isRPostgreSQL(conn)) {
    dbExecute(conn, "SET SESSION timezone TO 'UTC'")
  } else if (isRMySQL(conn)) {
    dbExecute(conn, "SET time_zone = '+00:00'")
  }

  conn
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
#' @importFrom DBI dbSendQuery dbFetch dbClearResult dbHasCompleted dbColumnInfo
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' DBI::dbCreateTable(db, "forecasts", data.frame(id=1:3, temperature=20:22))
#'
#' records <- dbxSelect(db, "SELECT * FROM forecasts")
dbxSelect <- function(conn, statement) {
  statement <- processStatement(statement)
  ret <- list()
  cast_dates <- list()
  cast_datetimes <- list()
  convert_tz <- list()
  cast_booleans <- list()
  stringify_json <- list()
  cast_json <- list()
  cast_times <- list()
  unescape_blobs <- list()
  cast_blobs <- list()

  silenceWarnings(c("length of NULL cannot be changed", "unrecognized MySQL field type", "unrecognized PostgreSQL field type", "(unknown ("), {
    res <- dbSendQuery(conn, statement)

    if (isRPostgreSQL(conn)) {
      column_info <- dbColumnInfo(res)
      sql_types <- tolower(column_info$type)

      if (storageTimeZone(conn) != currentTimeZone()) {
        convert_tz <- which(sql_types == "timestamp")
      }

      if (identical(attr(conn, "dbx_cast_times"), TRUE)) {
        cast_times <- which(sql_types == "time")
      }

      unescape_blobs <- which(sql_types == "bytea")
      if (identical(attr(conn, "dbx_cast_blobs"), TRUE)) {
        cast_blobs <- unescape_blobs
      }

      # json columns come back as unknown in RPostgreSQL
      # could try to parse them from warning messages generated
      # cast_json <- which(sql_types %in% c("json", "jsonb"))
    } else if (isRPostgres(conn)) {
      column_info <- dbColumnInfo(res)
      sql_types <- column_info$`.typname`

      if (storageTimeZone(conn) != currentTimeZone()) {
        convert_tz <- which(sql_types == "timestamp")
      }

      stringify_json <- which(sql_types %in% c("json", "jsonb"))
      cast_json <- stringify_json
    } else if (isRMySQL(conn)) {
      column_info <- dbColumnInfo(res)
      sql_types <- tolower(column_info$type)

      cast_dates <- which(sql_types == "date")
      cast_datetimes <- which(sql_types %in% c("datetime", "timestamp"))
      cast_booleans <- which(sql_types == "tinyint" & column_info$length == 1)

      if (identical(attr(conn, "dbx_cast_times"), TRUE)) {
        cast_times <- which(sql_types == "time")
      }

      # cast_json <- which(sql_types == "json")
    }

    # always fetch at least once
    ret[[length(ret) + 1]] <- dbFetch(res)

    while (!dbHasCompleted(res)) {
      ret[[length(ret) + 1]] <- dbFetch(res)
    }
    dbClearResult(res)
  })

  records <- combineResults(ret)

  if (nrow(records) > 0) {
    if (isRMariaDB(conn)) {
      # TODO cast booleans for RMariaDB
      # waiting on https://github.com/r-dbi/RMariaDB/issues/100

      cast_blobs <- which(sapply(records, isBinary))
    } else if (isSQLite(conn)) {
      # TODO cast dates and times for RSQLite
      # waiting on https://github.com/r-dbi/RSQLite/issues/263

      if (identical(attr(conn, "dbx_cast_blobs"), TRUE)) {
        cast_blobs <- which(sapply(records, isBinary))
      }
    }

    for (i in cast_dates) {
      records[, i] <- as.Date(records[, i])
    }

    for (i in cast_datetimes) {
      records[, i] <- as.POSIXct(records[, i], tz=storageTimeZone(conn))
      attr(records[, i], "tzone") <- currentTimeZone()
    }

    for (i in convert_tz) {
      records[, i] <- as.POSIXct(format(records[, i], "%Y-%m-%d %H:%M:%OS6"), tz=storageTimeZone(conn))
      attr(records[, i], "tzone") <- currentTimeZone()
    }

    for (i in stringify_json) {
      records[, i] <- as.character(records[, i])
    }

    if (identical(attr(conn, "dbx_cast_json"), TRUE)) {
      for (i in cast_json) {
        records[[colnames(records)[i]]] <- lapply(records[, i], function(x) { if (is.na(x)) x else jsonlite::fromJSON(x) })
      }
    }

    for (i in cast_booleans) {
      records[, i] <- records[, i] != 0
    }

    for (i in unescape_blobs) {
      records[[colnames(records)[i]]] <- lapply(records[, i], function(x) { if (is.na(x)) x else RPostgreSQL::postgresqlUnescapeBytea(x) })
    }

    for (i in cast_blobs) {
      records[[colnames(records)[i]]] <- blob::as.blob(records[, i])
    }

    for (i in cast_times) {
      records[, i] <- hms::as.hms(records[, i])
    }

    if (identical(attr(conn, "dbx_cast_times"), FALSE)) {
      uncast_times <- which(sapply(records, isTime))
      for (i in uncast_times) {
        records[, i] <- format(records[, i])
      }
    }

    if (identical(attr(conn, "dbx_cast_blobs"), FALSE)) {
      uncast_blobs <- which(sapply(records, isBlob))
      for (i in uncast_blobs) {
        records[[colnames(records)[i]]] <- lapply(records[, i], as.raw)
      }
    }
  }

  records
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

findAdapter <- function(adapter) {
  obj <- NULL
  if (!is.character(adapter)) {
    obj <- adapter
  } else {
    adapter <- tolower(adapter)
    if (adapter == "rsqlite") {
      requireLib("RSQLite")
      obj <- RSQLite::SQLite()
    } else if (adapter == "rmariadb") {
      requireLib("RMariaDB")
      obj <- RMariaDB::MariaDB()
    } else if (adapter == "rmysql") {
      requireLib("RMySQL")
      obj <- RMySQL::MySQL()
    } else if (adapter == "rpostgres") {
      requireLib("RPostgres")
      obj <- RPostgres::Postgres()
    } else if (adapter == "rpostgresql") {
      requireLib("RPostgreSQL")
      obj <- RPostgreSQL::PostgreSQL()
    }
  }
  obj
}

silenceWarnings <- function(msgs, code) {
  warn <- function(w) {
    if (any(sapply(msgs, function(x) { grepl(x, conditionMessage(w), fixed=TRUE) }))) {
      invokeRestart("muffleWarning")
    }
  }
  withCallingHandlers(code, warning=warn)
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

isRPostgreSQL <- function(conn) {
  inherits(conn, "PostgreSQLConnection")
}

isRPostgres <- function(conn) {
  inherits(conn, "PqConnection")
}

isPostgres <- function(conn) {
  isRPostgreSQL(conn) || isRPostgres(conn)
}

isRMySQL <- function(conn) {
  inherits(conn, "MySQLConnection")
}

isMySQL <- function(conn) {
  isRMySQL(conn) || isRMariaDB(conn)
}

isRMariaDB <- function(conn) {
  inherits(conn, "MariaDBConnection")
}

isSQLite <- function(conn) {
  inherits(conn, "SQLiteConnection")
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
  if (isNamespaceLoaded("dplyr") && exists("bind_rows", where="package:dplyr", mode="function")) {
    dplyr::bind_rows(ret)
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

#' @importFrom DBI dbQuoteIdentifier
quoteIdent <- function(conn, cols) {
  as.character(dbQuoteIdentifier(conn, cols))
}

#' @importFrom DBI dbQuoteLiteral
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
      } else if (isLogical(col) && isRPostgreSQL(conn)) {
        col <- as.character(col)
      } else if (isBinary(col)) {
        if (isRPostgreSQL(conn)) {
          col <- as.character(lapply(col, function(x) { RPostgreSQL::postgresqlEscapeBytea(conn, x) }))
        }
      }
    } else if (isSQLite(conn)) {
      # since no standard, store dates and times in the same format as Rails
      if (isDatetime(col)) {
        col <- format(col, tz=storageTimeZone(conn), "%Y-%m-%d %H:%M:%OS6")
      } else if (isDate(col)) {
        col <- format(col)
      }
    }
    quoted_records[, i] <- dbQuoteLiteral(conn, col)
  }
  quoted_records
}
