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
    if (isPostgres(conn) || isMySQL(conn) || isDuckDB(conn)) {
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
