#' Execute statement
#'
#' @param conn A DBIConnection object
#' @param statement The SQL statement to use
#' @param params Parameters to bind
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' DBI::dbCreateTable(db, "forecasts", data.frame(id=1:3, temperature=20:22))
#'
#' dbxExecute(db, "UPDATE forecasts SET temperature = 20")
#'
#' dbxExecute(db, "UPDATE forecasts SET temperature = ?", params=list(20))
#'
#' dbxExecute(db, "UPDATE forecasts SET temperature = ? WHERE id IN (?)", params=list(20, 1:3))
dbxExecute <- function(conn, statement, params=NULL) {
  statement <- addParams(conn, statement, params)
  execute(conn, statement)
}
