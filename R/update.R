#' Update records
#'
#' @param conn A DBIConnection object
#' @param table The table name to update
#' @param records A data frame of records to insert
#' @param where_cols The columns to use for WHERE clause
#' @param batch_size The number of records to update in a single transaction (defaults to all)
#' @param transaction Wrap the update in a transaction (defaults to true)
#' @param fast Use a single query (defaults to false)
#' @export
#' @examples
#' db <- dbxConnect(adapter="sqlite", dbname=":memory:")
#' table <- "forecasts"
#' DBI::dbCreateTable(db, table, data.frame(id=1:3, temperature=20:22))
#'
#' records <- data.frame(id=c(1, 2), temperature=c(16, 13))
#' dbxUpdate(db, table, records, where_cols=c("id"))
dbxUpdate <- function(conn, table, records, where_cols, batch_size=NULL, transaction=TRUE, fast=FALSE) {
  cols <- colnames(records)

  if (!setequal(intersect(cols, where_cols), where_cols)) {
    stop("where_cols not in records")
  }

  update_cols <- setdiff(cols, where_cols)

  # quote
  quoted_table <- quoteIdent(conn, table)
  quoted_where_cols <- quoteIdent(conn, where_cols)
  quoted_update_cols <- quoteIdent(conn, update_cols)

  if (fast) {
    if (!isPostgres(conn) && !isSQLServer(conn)) {
      stop("fast is only supported with Postgres and SQL Server")
    }

    quoted_cols <- quoteIdent(conn, cols)
    set_sql <- fastUpdateSetClausePostgres(quoted_update_cols)
    cols_sql <- colsClause(quoted_cols)
    where_sql <- fastUpdateWhereClausePostgres(quoted_where_cols)

    inBatches(records, batch_size, function(batch) {
      if (isSQLServer(conn)) {
        sql <- paste0("UPDATE t SET ", set_sql, " FROM ", quoted_table, " AS t, (VALUES ", valuesClause(conn, batch), ") AS s (", cols_sql, ") WHERE ", where_sql)
      } else {
        sql <- paste0("UPDATE ", quoted_table, " AS t SET ", set_sql, " FROM (VALUES ", valuesClause(conn, batch), ") AS s (", cols_sql, ") WHERE ", where_sql)
      }
      execute(conn, sql)
    })
  } else {
    inBatches(records, batch_size, function(batch) {
      quoted_records <- quoteRecords(conn, batch)
      colnames(quoted_records) <- colnames(batch)
      groups <- split(quoted_records, quoted_records[update_cols], drop=TRUE)

      maybeTransaction(transaction, conn, {
        for (group in groups) {
          row <- group[1, , drop=FALSE]
          sql <- paste("UPDATE", quoted_table, "SET", setClause(quoted_update_cols, row[update_cols]), "WHERE", whereClause(quoted_where_cols, group[where_cols]))
          execute(conn, sql)
        }
      })
    })
  }

  invisible()
}

maybeTransaction <- function(transaction, conn, code) {
  if (isTRUE(transaction)) {
    withTransaction(conn, code)
  } else {
    eval(code)
  }
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
