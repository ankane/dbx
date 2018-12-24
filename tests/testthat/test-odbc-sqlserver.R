context("odbc-sqlserver")

skip("odbc")

db <- dbxConnect(adapter=odbc::odbc(), driver="/usr/local/lib/libtdsodbc.so", database="dbx_test", server="localhost", port=1433, uid="SA", pwd="YourNewStrong!Passw0rd")

dbxExecute(db, "DROP TABLE IF EXISTS events")
dbxExecute(db, "CREATE TABLE events (id INT NOT NULL IDENTITY PRIMARY KEY, city VARCHAR(255), counter INT, bigcounter BIGINT, speed FLOAT, distance DECIMAL(5, 2), created_on DATE, updated_at DATETIME2(6), deleted_at DATETIME2(6), open_time TIME, active BIT, properties TEXT, propertiesb TEXT, image IMAGE)")

runTests(db)
