context("odbc-mysql")

skip("odbc")

db <- dbxConnect(adapter=odbc::odbc(), driver="/usr/local/lib/libmaodbc.dylib", database="dbx_test")

dbxExecute(db, "DROP TABLE IF EXISTS events")
json_type <- if (Sys.getenv("TRAVIS") == "") "JSON" else "TEXT"
dbxExecute(db, paste0("CREATE TABLE events (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, city VARCHAR(255), counter INT, bigcounter BIGINT, speed FLOAT, distance DECIMAL(5, 2), created_on DATE, updated_at DATETIME(6), deleted_at TIMESTAMP(6) NULL DEFAULT NULL, open_time TIME, active BOOLEAN, properties ", json_type, ", image BLOB)"))

runTests(db)
