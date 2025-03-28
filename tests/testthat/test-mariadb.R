context("mariadb")

skip_on_cran()

# prevent conflict with MySQLConnection
unloadNamespace("RMySQL")

db <- dbxConnect(adapter="rmariadb", dbname="dbx_test")

dbxExecute(db, "DROP TABLE IF EXISTS events")
dbxExecute(db, "CREATE TABLE events (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, city VARCHAR(255), counter INT, bigcounter BIGINT, speed FLOAT, distance DECIMAL(20, 18), created_on DATE, updated_at DATETIME(6), deleted_at TIMESTAMP(6) NULL DEFAULT NULL, open_time TIME(3), properties TEXT, active BOOLEAN, image BLOB)")

runTests(db)
