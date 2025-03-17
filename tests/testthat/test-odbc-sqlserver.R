context("odbc-sqlserver")

skip("odbc")

# brew install freetds
db <- dbxConnect(adapter=odbc::odbc(), driver="/opt/homebrew/lib/libtdsodbc.so", database="dbx_test", server="localhost", port=1433, uid="SA", pwd="YourStrong!Passw0rd")

dbxExecute(db, "DROP TABLE IF EXISTS events")
dbxExecute(db, "DROP SEQUENCE IF EXISTS events_id_seq")

dbxExecute(db, "CREATE SEQUENCE events_id_seq")
dbxExecute(db, "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY CONSTRAINT c DEFAULT NEXT VALUE FOR events_id_seq, city VARCHAR(255), counter INT, bigcounter BIGINT, speed FLOAT, distance DECIMAL(5, 2), created_on DATE, updated_at DATETIME2(6), deleted_at DATETIME2(6), open_time TIME, active BIT, properties TEXT, propertiesb TEXT, image IMAGE)")

runTests(db)
