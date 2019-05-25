#' Update records
#'
#' @param conn A DBIConnection object
#' @param table The table name to update
#' @param records A data frame of records to insert
#' @param where_cols The columns to use for WHERE clause
#' @param batch_size The number of records to update in a single transaction (defaults to all)
#' @param allow_transaction_inside If true, do the update in a transactional block; otherwise, don't
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#' DBI::dbCreateTable(db, table, data.frame(id=1:3, temperature=20:22))
#'
#' records <- data.frame(id=c(1, 2), temperature=c(16, 13))
#' dbxUpdate(db, table, records, where_cols=c("id"))
dbxUpdate <- function(conn, table, records, where_cols, batch_size=NULL, allow_transaction_inside=TRUE) {
  cols <- colnames(records)

  if (!setequal(intersect(cols, where_cols), where_cols)) {
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
    groups <- split(quoted_records, quoted_records[update_cols], drop=TRUE)

    update_execution <- function(conn, groups, quoted_table, quoted_update_cols, update_cols, quoted_where_cols, where_cols) {
      for (group in groups) {
        row <- group[1, , drop=FALSE]
        sql <- paste("UPDATE", quoted_table, "SET", setClause(quoted_update_cols, row[update_cols]), "WHERE", whereClause(quoted_where_cols, group[where_cols]))
        execute(conn, sql)
      }
    }
    
    if (allow_transaction_inside) {
      withTransaction(conn, {
        update_execution(conn, groups, quoted_table, quoted_update_cols, update_cols, quoted_where_cols, where_cols)
      })
     } else {
        update_execution(conn, groups, quoted_table, quoted_update_cols, update_cols, quoted_where_cols, where_cols)
     }
  })

  invisible()
}

withTransaction <- function(conn, code) {
  if (isSQLServer(conn)) {
    DBI::dbWithTransaction(conn, code)
  } else {
    execute(conn, "BEGIN")
    tryCatch({
      eval(code)
      execute(conn, "COMMIT")
    }, error=function(err) {
      execute(conn, "ROLLBACK")
      stop(err)
    })
  }
}
