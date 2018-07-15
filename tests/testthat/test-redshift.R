context("redshift")

skip("redshift")

db <- dbxConnect(url="rpostgresql://...")

dbExecute(db, "DROP TABLE IF EXISTS orders")
dbExecute(db, "CREATE TABLE orders (id INT, city text, other text, PRIMARY KEY (id))")

dbExecute(db, "DROP TABLE IF EXISTS events")
dbExecute(db, "CREATE TABLE events (id INT IDENTITY(0, 1), created_on DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ, open_time TEXT, close_time TEXT, active BOOLEAN, properties TEXT, propertiesb TEXT, image TEXT, PRIMARY KEY(id))")

runTests(db, redshift=TRUE)
