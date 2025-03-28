#' Insert records
#'
#' @param conn A DBIConnection object
#' @param table The table name to insert
#' @param records A data frame of records to insert
#' @param batch_size The number of records to insert in a single statement (defaults to all)
#' @param returning Columns to return
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#' DBI::dbCreateTable(db, table, data.frame(id=1:3, temperature=20:22))
#'
#' records <- data.frame(temperature=c(32, 25))
#' dbxInsert(db, table, records)
dbxInsert <- function(conn, table, records, batch_size=NULL, returning=NULL) {
  inBatches(records, batch_size, function(batch) {
    sql <- insertClause(conn, table, batch, returning=returning)
    selectOrExecute(conn, sql, batch, returning=returning)
  })
}
