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
  inherits(conn, "MySQLConnection") && !inherits(conn, "MariaDBConnection")
}

isMySQL <- function(conn) {
  isRMySQL(conn) || isRMariaDB(conn) || isODBCMySQL(conn)
}

isRMariaDB <- function(conn) {
  inherits(conn, "MariaDBConnection")
}

isMariaDB <- function(conn) {
  # RMariaDB uses MySQLConnection for MySQL for 1.3+
  # TODO add detection for other adapters
  inherits(conn, "MariaDBConnection") && !is(conn, "MySQLConnection")
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
