isPostgres <- function(conn) {
  isRPostgreSQL(conn) || isRPostgres(conn) || isODBCPostgres(conn)
}

isRPostgreSQL <- function(conn) {
  inherits(conn, "PostgreSQLConnection")
}

isRPostgres <- function(conn) {
  inherits(conn, "PqConnection")
}

isODBCPostgres <- function(conn) {
  inherits(conn, "PostgreSQL")
}

isRMySQL <- function(conn) {
  inherits(conn, "MySQLConnection")
}

isMySQL <- function(conn) {
  isRMySQL(conn) || isRMariaDB(conn) || isODBCMySQL(conn)
}

isRMariaDB <- function(conn) {
  inherits(conn, "MariaDBConnection")
}

isSQLite <- function(conn) {
  inherits(conn, "SQLiteConnection")
}

isODBCMySQL <- function(conn) {
  inherits(conn, "MySQL")
}

isSQLServer <- function(conn) {
  inherits(conn, "Microsoft SQL Server")
}

isODBC <- function(conn) {
  !is.null(attr(conn, "info")$odbc.version)
}
