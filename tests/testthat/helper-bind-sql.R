bindSQL <- function(db, sql) {
  if (!isPostgres(db)) {
    sql <- gsub("\\$\\d+", "?", sql)
  }
  sql
}
