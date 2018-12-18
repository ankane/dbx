context("odbc-postgres")

skip("odbc")

db <- dbxConnect(adapter=odbc::odbc(), driver="/usr/local/lib/psqlodbca.so", database="dbx_test")

dbxExecute(db, "DROP TABLE IF EXISTS events")
dbxExecute(db, "CREATE TABLE events (id SERIAL PRIMARY KEY, city text, counter INT, speed FLOAT, distance DECIMAL, created_on DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ, open_time TIME, close_time TIMETZ, active BOOLEAN, properties JSON, propertiesb JSONB, image BYTEA)")

runTests(db)
