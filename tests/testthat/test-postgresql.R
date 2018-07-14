context("postgresql")

skip_on_cran()

db <- dbxConnect(adapter="rpostgresql", dbname="dbx_test")

dbExecute(db, "DROP TABLE IF EXISTS orders")
dbExecute(db, "CREATE TABLE orders (id SERIAL PRIMARY KEY, city text, other text)")

dbExecute(db, "DROP TABLE IF EXISTS events")
dbExecute(db, "CREATE TABLE events (id SERIAL PRIMARY KEY, created_on DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ, open_time TIME, close_time TIMETZ, active BOOLEAN, properties JSON, propertiesb JSONB, image BYTEA)")

runTests(db)
