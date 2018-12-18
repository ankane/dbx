context("mysql")

skip_on_cran()

db <- dbxConnect(adapter="rmysql", dbname="dbx_test")

dbxExecute(db, "DROP TABLE IF EXISTS events")
json_type <- if (Sys.getenv("TRAVIS") == "") "JSON" else "TEXT"
dbxExecute(db, paste0("CREATE TABLE events (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, city VARCHAR(255), counter INT, speed FLOAT, distance DECIMAL(5, 2), created_on DATE, updated_at DATETIME(6), deleted_at TIMESTAMP(6) NULL DEFAULT NULL, open_time TIME, active BOOLEAN, properties ", json_type, ", image BLOB)"))

runTests(db)

test_that("connect with url works", {
  con2 <- dbxConnect("mysql://localhost/dbx_test")
  res <- dbxSelect(con2, "SELECT 1 AS hi")
  dbxDisconnect(con2)
  exp <- data.frame(hi=c(1))
  expect_equal(res, exp)
})
