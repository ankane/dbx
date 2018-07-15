isRPostgreSQL <- function(conn) {
  inherits(conn, "PostgreSQLConnection")
}

isRPostgres <- function(conn) {
  inherits(conn, "PqConnection")
}

isPostgres <- function(conn) {
  isRPostgreSQL(conn) || isRPostgres(conn)
}

isRMySQL <- function(conn) {
  inherits(conn, "MySQLConnection")
}

isMySQL <- function(conn) {
  isRMySQL(conn) || isRMariaDB(conn)
}

isRMariaDB <- function(conn) {
  inherits(conn, "MariaDBConnection")
}

isSQLite <- function(conn) {
  inherits(conn, "SQLiteConnection")
}