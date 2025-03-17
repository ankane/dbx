context("odbc-postgres")

skip("odbc")

if (isMac()) {
  # brew install psqlodbc
  driver <- "/opt/homebrew/lib/psqlodbca.so"
} else {
  # apt-get install odbc-postgresql
  driver <- "/usr/lib/x86_64-linux-gnu/odbc/psqlodbca.so"
}

db <- dbxConnect(adapter=odbc::odbc(), driver=driver, database="dbx_test")

dbxExecute(db, "DROP TABLE IF EXISTS events")
dbxExecute(db, "CREATE TABLE events (id SERIAL PRIMARY KEY, city text, counter INT, bigcounter BIGINT, speed FLOAT, distance DECIMAL, created_on DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ, open_time TIME, close_time TIMETZ, active BOOLEAN, properties JSON, propertiesb JSONB, image BYTEA)")

runTests(db)
