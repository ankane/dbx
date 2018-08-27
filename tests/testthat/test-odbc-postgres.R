context("odbc-postgres")

skip_on_cran()
skip_on_travis()

db <- dbxConnect(adapter=odbc::odbc(),
  driver = "/usr/local/Cellar/psqlodbc/10.03.0000/lib/psqlodbca.so",
  database = "dbx_test",
  host = "localhost",
  port = 5432
)

dbExecute(db, "DROP TABLE IF EXISTS orders")
dbExecute(db, "CREATE TABLE orders (id SERIAL PRIMARY KEY, city text)")

dbExecute(db, "DROP TABLE IF EXISTS events")
dbExecute(db, "CREATE TABLE events (id SERIAL PRIMARY KEY, counter INT, speed FLOAT, distance DECIMAL, created_on DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ, open_time TIME, close_time TIMETZ, active BOOLEAN, properties JSON, propertiesb JSONB, image BYTEA)")

runTests(db)