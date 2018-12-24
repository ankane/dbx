#' Select records
#'
#' @param conn A DBIConnection object
#' @param statement The SQL statement to use
#' @param params Parameters to bind
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' DBI::dbCreateTable(db, "forecasts", data.frame(id=1:3, temperature=20:22))
#'
#' dbxSelect(db, "SELECT * FROM forecasts")
#'
#' dbxSelect(db, "SELECT * FROM forecasts WHERE id = ?", params=list(1))
#'
#' dbxSelect(db, "SELECT * FROM forecasts WHERE id IN (?)", params=list(1:3))
dbxSelect <- function(conn, statement, params=NULL) {
  statement <- processStatement(statement)
  cast_dates <- list()
  cast_datetimes <- list()
  convert_tz <- list()
  cast_booleans <- list()
  stringify_json <- list()
  unescape_blobs <- list()
  fix_timetz <- list()
  change_tz <- list()
  check_bigint <- list()

  r <- fetchRecords(conn, statement, params)
  records <- r$records
  column_info <- r$column_info

  # typecasting
  if (isRPostgreSQL(conn)) {
    sql_types <- tolower(column_info$type)

    if (storageTimeZone(conn) != currentTimeZone()) {
      convert_tz <- which(sql_types == "timestamp")
    }

    unescape_blobs <- which(sql_types == "bytea")
    fix_timetz <- which(sql_types == "timetzoid")
    check_bigint <- which(sql_types == "bigint")
  } else if (isRPostgres(conn)) {
    sql_types <- column_info$`.typname`

    if (storageTimeZone(conn) != currentTimeZone()) {
      convert_tz <- which(sql_types == "timestamp")
    }

    stringify_json <- which(sql_types %in% c("json", "jsonb"))
  } else if (isRMySQL(conn)) {
    sql_types <- tolower(column_info$type)

    cast_dates <- which(sql_types == "date")
    cast_datetimes <- which(sql_types %in% c("datetime", "timestamp"))
    cast_booleans <- which(sql_types == "tinyint" & column_info$length == 1)
    check_bigint <- which(sql_types == "bigint")
  } else if (isRMariaDB(conn)) {
    # TODO cast booleans for RMariaDB
    # waiting on https://github.com/r-dbi/RMariaDB/issues/100
  } else if (isSQLite(conn)) {
    # TODO cast dates and times for RSQLite
    # waiting on https://github.com/r-dbi/RSQLite/issues/263
  } else if (isODBC(conn)) {
    # TODO cast booleans for Postgres ODBC
    # https://github.com/r-dbi/odbc/issues/108
    # booleans currently returned as VARCHAR
    # print(column_info)
    sql_types = column_info$type
    change_tz <- which(sql_types == 93)
    cast_booleans <- which(sql_types == -6)
  }

  # fix for empty data frame
  # until new RPostgreSQL version is published
  # https://github.com/tomoakin/RPostgreSQL/commit/f93cb17cf584d57ced5045a46d16d2bfe05a2769
  if (isRPostgreSQL(conn) && ncol(records) == 0) {
    for (i in 1:nrow(column_info)) {
      row <- column_info[i, ]
      records[, i] <- emptyType(row$Sclass)
    }
    colnames(records) <- column_info$name

    for (i in unescape_blobs) {
      records[[colnames(records)[i]]] <- list()
    }
  }

  for (i in cast_booleans) {
    records[, i] <- records[, i] != 0
  }

  for (i in stringify_json) {
    records[, i] <- as.character(records[, i])
  }

  for (i in change_tz) {
    attr(records[, i], "tzone") <- currentTimeZone()
  }

  bigint_message <- "bigint value outside range of numeric"
  for (i in check_bigint) {
    max_bigint <- 9007199254740992 # 2**53
    col <- records[, i]
    if (any(!is.na(col) & (col >= max_bigint | col <= -max_bigint))) {
      stop(bigint_message)
    }
  }

  if (isTRUE(attr(conn, "dbx_cast_bigint"))) {
    for (i in 1:ncol(records)) {
      col <- records[, i]
      if (inherits(col, "integer64")) {
        tryCatch({
          records[, i] <- as.numeric(col)
        }, warning=function(w) {
          if (grepl("integer precision lost", conditionMessage(w), fixed=TRUE)) {
            stop(bigint_message)
          }
        })
      }
    }
  }

  if (nrow(records) > 0) {
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

    for (i in unescape_blobs) {
      records[[colnames(records)[i]]] <- lapply(records[, i], function(x) { if (is.na(x)) as.raw(NULL) else RPostgreSQL::postgresqlUnescapeBytea(x) })
    }

    for (i in fix_timetz) {
      records[, i] <- gsub("\\+00$", "", records[, i])
    }

    uncast_times <- which(sapply(records, isTime))
    for (i in uncast_times) {
      records[, i] <- as.character(records[, i])
    }

    uncast_blobs <- which(sapply(records, isBlob))
    for (i in uncast_blobs) {
      records[[colnames(records)[i]]] <- lapply(records[, i], as.raw)
    }
  } else {
    for (i in cast_dates) {
      records[, i] <- as.Date(as.character())
    }

    for (i in cast_datetimes) {
      records[, i] <- as.POSIXct(as.character())
    }

    uncast_times <- which(sapply(records, isTime))
    for (i in uncast_times) {
      records[, i] <- as.character()
    }

    uncast_blobs <- which(sapply(records, isBlob))
    for (i in uncast_blobs) {
      records[[colnames(records)[i]]] <- list()
    }
  }

  records
}

fetchRecords <- function(conn, statement, params) {
  ret <- list()
  column_info <- NULL

  silenceWarnings(c("length of NULL cannot be changed", "unrecognized MySQL field type", "unrecognized PostgreSQL field type", "(unknown (", "Decimal MySQL column"), {
    statement <- addParams(conn, statement, params)

    res <- NULL
    timeStatement(statement, {
      res <- DBI::dbSendQuery(conn, statement)
    })

    # always fetch at least once
    ret[[length(ret) + 1]] <- DBI::dbFetch(res)

    # must come after first fetch call for SQLite
    column_info <- DBI::dbColumnInfo(res)

    while (!DBI::dbHasCompleted(res)) {
      ret[[length(ret) + 1]] <- DBI::dbFetch(res)
    }
    DBI::dbClearResult(res)
  })

  list(records=combineResults(ret), column_info=column_info)
}

emptyType <- function(type) {
  if (type == "Date") {
    as.Date(as.character())
  } else if (type == "POSIXct") {
    as.POSIXct(as.character())
  } else if (type == "integer") {
    as.integer()
  } else if (type == "numeric") {
    as.numeric()
  } else if (type == "double") {
    as.double()
  } else if (type == "logical") {
    as.logical()
  } else {
    as.character()
  }
}

silenceWarnings <- function(msgs, code) {
  warn <- function(w) {
    if (any(sapply(msgs, function(x) { grepl(x, conditionMessage(w), fixed=TRUE) }))) {
      invokeRestart("muffleWarning")
    }
  }
  withCallingHandlers(code, warning=warn)
}
