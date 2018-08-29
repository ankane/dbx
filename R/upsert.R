#' Upsert records
#'
#' @param conn A DBIConnection object
#' @param table The table name to upsert
#' @param records A data frame of records to upsert
#' @param where_cols The columns to use for WHERE clause
#' @param batch_size The number of records to upsert in a single statement (defaults to all)
#' @param returning Columns to return
#' @export
#' @examples \dontrun{
#'
#' db <- dbxConnect(adapter="postgres", dbname="dbx")
#' table <- "forecasts"
#' DBI::dbCreateTable(db, table, data.frame(id=1:3, temperature=20:22))
#'
#' records <- data.frame(id=c(3, 4), temperature=c(20, 25))
#' dbxUpsert(db, table, records, where_cols=c("id"))
#' }
dbxUpsert <- function(conn, table, records, where_cols, batch_size=NULL, returning=NULL) {
  cols <- colnames(records)

  if (!identical(intersect(cols, where_cols), where_cols)) {
    stop("where_cols not in records")
  }

  update_cols <- setdiff(cols, where_cols)
  if (length(update_cols) == 0) {
    update_cols <- where_cols[1]
  }

  # quote
  quoted_where_cols <- quoteIdent(conn, where_cols)
  quoted_update_cols <- quoteIdent(conn, update_cols)

  inBatches(records, batch_size, function(batch) {
    if (isMySQL(conn)) {
      sql <- insertClause(conn, table, batch)
      set_sql <- upsertSetClause(quoted_update_cols)
      sql <- paste(sql, "ON DUPLICATE KEY UPDATE", set_sql)
      selectOrExecute(conn, sql, batch, returning=returning)
    } else {
      conflict_target <- colsClause(quoted_where_cols)
      sql <- insertClause(conn, table, batch)
      set_sql <- upsertSetClausePostgres(quoted_update_cols)
      sql <- paste0(sql, " ON CONFLICT (", conflict_target, ") DO UPDATE SET ", set_sql)
      selectOrExecute(conn, sql, batch, returning=returning)
    }
  })
}
