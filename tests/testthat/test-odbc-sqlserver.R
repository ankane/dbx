context("odbc-sqlserver")

skip("odbc")

db <- dbxConnect(adapter=odbc::odbc(), driver="/usr/local/lib/libtdsodbc.so", database="dbx_test", server="localhost", port=1433, uid="SA", pwd="YourNewStrong!Passw0rd")

dbExecute(db, "DROP TABLE IF EXISTS orders")
dbExecute(db, "CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, city VARCHAR(255))")

dbExecute(db, "DROP TABLE IF EXISTS events")
dbExecute(db, "CREATE TABLE events (id INT NOT NULL IDENTITY PRIMARY KEY, counter INT, speed FLOAT, distance DECIMAL(5, 2), created_on DATE, updated_at DATETIME2(6), deleted_at DATETIME2(6), open_time TIME, active BIT, properties TEXT, propertiesb TEXT, image IMAGE)")

runTests(db)
