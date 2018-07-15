context("mariadb")

skip_on_cran()

db <- dbxConnect(adapter="rmariadb", dbname="dbx_test")

dbExecute(db, "DROP TABLE IF EXISTS orders")
dbExecute(db, "CREATE TABLE orders (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, city VARCHAR(255))")

dbExecute(db, "DROP TABLE IF EXISTS events")
dbExecute(db, "CREATE TABLE events (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, created_on DATE, updated_at DATETIME(6), deleted_at TIMESTAMP(6) NULL DEFAULT NULL, open_time TIME, properties TEXT, active BOOLEAN, image BLOB)")

runTests(db)
