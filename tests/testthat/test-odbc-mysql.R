context("odbc-mysql")

skip("odbc")

if (isMac()) {
  # brew install mariadb-connector-odbc
  driver <- "/opt/homebrew/lib/mariadb/libmaodbc.dylib"
} else {
  # apt-get install odbc-mariadb
  driver <- "/usr/lib/x86_64-linux-gnu/odbc/libmaodbc.so"
}

db <- dbxConnect(adapter=odbc::odbc(), driver=driver, database="dbx_test")

dbxExecute(db, "DROP TABLE IF EXISTS events")
dbxExecute(db, paste0("CREATE TABLE events (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, city VARCHAR(255), counter INT, bigcounter BIGINT, speed FLOAT, distance DECIMAL(20, 18), created_on DATE, updated_at DATETIME(6), deleted_at TIMESTAMP(6) NULL DEFAULT NULL, open_time TIME, active BOOLEAN, properties JSON, image BLOB)"))

runTests(db)
