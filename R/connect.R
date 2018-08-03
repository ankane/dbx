#' Create a database connection
#'
#' @param url A database URL
#' @param adapter The database adapter to use
#' @param storage_tz The time zone timestamps are stored in
#' @param ... Arguments to pass to dbConnect
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
dbxConnect <- function(url=NULL, adapter=NULL, storage_tz=NULL, ...) {
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

  if (inherits(obj, "PostgreSQLDriver")) {
    if (!is.null(params$sslmode) || !is.null(params$sslrootcert)) {
      dbname <- list(dbname=params$dbname)
      if (!is.null(params$sslmode)) {
        dbname$sslmode <- params$sslmode
        params$sslmode <- NULL
      }
      if (!is.null(params$sslrootcert)) {
        dbname$sslrootcert <- params$sslrootcert
        params$sslrootcert <- NULL
      }
      params$dbname <- toConnStr(dbname)
    }
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

  # other adapters do this automatically
  if (isRPostgreSQL(conn)) {
    dbExecute(conn, "SET timezone TO 'UTC'")
  } else if (isRMySQL(conn)) {
    dbExecute(conn, "SET time_zone = '+00:00'")
  }

  conn
}

# escape connection string
# https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING
# To write an empty value, or a value containing spaces, surround it with single quotes,
# e.g., keyword = 'a value'. Single quotes and backslashes within the value must be escaped
# with a backslash, i.e., \' and \\.
toConnStr <- function(x) {
  paste0(mapply(function(x, i) paste0(i, "='", gsub("'", "\\'", gsub("\\", "\\\\", x, fixed=TRUE), fixed=TRUE), "'"), x, names(x)), collapse=" ")
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
      obj <- RMySQL::MySQL(fetch.default.rec=10000)
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

requireLib <- function(name) {
  if (!requireNamespace(name, quietly=TRUE)) {
    stop(paste("Could not load adapter:", name))
  }
}