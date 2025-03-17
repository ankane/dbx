#' Upsert records
#'
#' @param conn A DBIConnection object
#' @param table The table name to upsert
#' @param records A data frame of records to upsert
#' @param where_cols The columns to use for WHERE clause
#' @param batch_size The number of records to upsert in a single statement (defaults to all)
#' @param returning Columns to return
#' @param skip_existing Skip existing rows
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
dbxUpsert <- function(conn, table, records, where_cols, batch_size=NULL, returning=NULL, skip_existing=FALSE) {
  cols <- colnames(records)

  if (!setequal(intersect(cols, where_cols), where_cols)) {
    stop("where_cols not in records")
  }

  update_cols <- setdiff(cols, where_cols)
  if (length(update_cols) == 0) {
    if (is.null(returning) || isDuckDB(conn)) {
      skip_existing <- TRUE
    } else {
      update_cols <- where_cols[1]
    }
  }

  # quote
  quoted_where_cols <- quoteIdent(conn, where_cols)
  quoted_update_cols <- quoteIdent(conn, update_cols)

  inBatches(records, batch_size, function(batch) {
    if (isMySQL(conn)) {
      sql <- insertClause(conn, table, batch)
      if (skip_existing) {
        # do not use INSERT IGNORE
        # https://stackoverflow.com/questions/2366813/on-duplicate-key-ignore
        set_sql <- upsertSetClause(quoted_where_cols)
      } else {
        set_sql <- upsertSetClause(quoted_update_cols)
      }
      sql <- paste(sql, "ON DUPLICATE KEY UPDATE", set_sql)
      selectOrExecute(conn, sql, batch, returning=returning)
    } else if (isSQLServer(conn)) {
      quoted_table <- quoteIdent(conn, table)
      quoted_cols <- quoteIdent(conn, cols)
      on_sql <- upsertOnClauseSQLServer(quoted_where_cols)

      sql <- paste0("MERGE ", quoted_table, " WITH (HOLDLOCK) AS t USING (VALUES ", valuesClause(conn, batch), ") AS s (", colsClause(quoted_cols), ") ON (", on_sql, ") WHEN NOT MATCHED BY TARGET THEN INSERT (", colsClause(quoted_cols), ") VALUES (", colsClause(quoted_cols), ")")
      if (!skip_existing) {
        set_sql <- upsertSetClauseSQLServer(quoted_update_cols)
        sql <- paste(sql, "WHEN MATCHED THEN UPDATE SET", set_sql)
      }

      output_sql <- ""
      if (!is.null(returning)) {
        output_sql <- outputClause(conn, returning)
      }

      selectOrExecute(conn, paste0(sql, output_sql, ";"), batch, returning=returning)
    } else {
      conflict_target <- colsClause(quoted_where_cols)
      sql <- insertClause(conn, table, batch)
      sql <- paste0(sql, " ON CONFLICT (", conflict_target, ") DO")
      if (skip_existing) {
        sql <- paste(sql, "NOTHING")
      } else {
        set_sql <- upsertSetClausePostgres(quoted_update_cols)
        sql <- paste(sql, "UPDATE SET", set_sql)
      }
      selectOrExecute(conn, sql, batch, returning=returning)
    }
  })
}
