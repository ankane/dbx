#' Select records
#'
#' @param conn A DBIConnection object
#' @param statement The SQL statement to use
#' @param cache Cache the result
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' DBI::dbCreateTable(db, "forecasts", data.frame(id=1:3, temperature=20:22))
#'
#' records <- dbxSelect(db, "SELECT * FROM forecasts")
dbxSelect <- function(conn, statement, cache=FALSE) {
  statement <- processStatement(statement)
  cast_dates <- list()
  cast_datetimes <- list()
  convert_tz <- list()
  cast_booleans <- list()
  stringify_json <- list()
  unescape_blobs <- list()
  fix_timetz <- list()

  r <- if (cache) fetchRecordsCached(conn, statement) else fetchRecords(conn, statement)
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
  } else if (isRMariaDB(conn)) {
    # TODO cast booleans for RMariaDB
    # waiting on https://github.com/r-dbi/RMariaDB/issues/100
  } else if (isSQLite(conn)) {
    # TODO cast dates and times for RSQLite
    # waiting on https://github.com/r-dbi/RSQLite/issues/263
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

statementCacheKey <- function(statement) {
  sum(utf8ToInt(statement))
}

fetchRecordsCached <- function(conn, statement) {
  # TODO add non-sensitive conn info
  cache_key <- paste0(statementCacheKey(statement), ".rds")
  # TODO use persistent dir
  cache_dir <- tempdir()

  cache_path <- file.path(cache_dir, cache_key)

  res <- NULL

  tryCatch({
    res <- suppressWarnings(readRDS(cache_path))
  }, error=function(err) {
    # file does not exist
    res <<- fetchRecords(conn, statement)
    saveRDS(res, cache_path)
  })

  res
}

fetchRecords <- function(conn, statement) {
  ret <- list()
  column_info <- NULL

  silenceWarnings(c("length of NULL cannot be changed", "unrecognized MySQL field type", "unrecognized PostgreSQL field type", "(unknown (", "Decimal MySQL column"), {
    res <- NULL
    timeStatement(statement, {
      res <- dbSendQuery(conn, statement)
    })

    # always fetch at least once
    ret[[length(ret) + 1]] <- dbFetch(res)

    # must come after first fetch call for SQLite
    column_info <- dbColumnInfo(res)

    while (!dbHasCompleted(res)) {
      ret[[length(ret) + 1]] <- dbFetch(res)
    }
    dbClearResult(res)
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
