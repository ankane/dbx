context("duckdb")

skip("duckdb")

db <- dbxConnect(adapter=duckdb::duckdb(), dbdir=":memory:")

dbxExecute(db, "CREATE SEQUENCE events_id_seq")
dbxExecute(db, "CREATE TABLE events (id INTEGER PRIMARY KEY DEFAULT nextval('events_id_seq'), city TEXT, counter INT, bigcounter BIGINT, speed FLOAT, distance DECIMAL, created_on DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ, open_time TIME, active BOOLEAN, properties TEXT, image BLOB)")

runTests(db)
