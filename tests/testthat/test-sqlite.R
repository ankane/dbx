context("sqlite")

db <- dbxConnect(adapter="rsqlite", dbname=":memory:")

dbExecute(db, "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, city VARCHAR(255), counter INT, speed FLOAT, distance DECIMAL, created_on DATE, updated_at DATETIME, open_time VARCHAR(255), active BOOLEAN, properties TEXT, image BLOB)")

runTests(db)

test_that("connect with url works", {
  con2 <- dbxConnect(url="sqlite:///:memory:")
  res <- dbxSelect(con2, "SELECT 1 AS hi")
  dbxDisconnect(con2)
  exp <- data.frame(hi=c(1))
  expect_equal(res, exp)
})
