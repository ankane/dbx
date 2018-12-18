context("redshift")

skip("redshift")

db <- dbxConnect(url="rpostgresql://...")

dbxExecute(db, "DROP TABLE IF EXISTS events")
dbxExecute(db, "CREATE TABLE events (id INT IDENTITY(0, 1), city TEXT, counter INT, speed FLOAT, distance DECIMAL, created_on DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ, open_time TEXT, close_time TEXT, active BOOLEAN, properties TEXT, propertiesb TEXT, image TEXT, PRIMARY KEY(id))")

runTests(db, redshift=TRUE)
