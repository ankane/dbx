#' Close a database connection
#'
#' @param conn A DBIConnection object
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#'
#' dbxDisconnect(db)
dbxDisconnect <- function(conn) {
  dbDisconnect(conn)
}
