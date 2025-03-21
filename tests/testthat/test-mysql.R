context("mysql")

skip_on_cran()

# prevent conflict with MySQLConnection
unloadNamespace("RMariaDB")

db <- dbxConnect(adapter="rmysql", dbname="dbx_test")

dbxExecute(db, "DROP TABLE IF EXISTS events")
dbxExecute(db, paste0("CREATE TABLE events (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, city VARCHAR(255), counter INT, bigcounter BIGINT, speed FLOAT, distance DECIMAL(20, 18), created_on DATE, updated_at DATETIME(6), deleted_at TIMESTAMP(6) NULL DEFAULT NULL, open_time TIME(3), active BOOLEAN, properties JSON, image BLOB)"))

runTests(db)

test_that("connect with url works", {
  con2 <- dbxConnect("mysql://localhost/dbx_test")
  res <- dbxSelect(con2, "SELECT 1 AS hi")
  dbxDisconnect(con2)
  exp <- data.frame(hi=c(1))
  expect_equal(res, exp)
})
