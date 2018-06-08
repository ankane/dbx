context("mysql")

skip_on_cran()

con <- dbxConnect(adapter="mysql", dbname="dbx_test")

orders <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
new_orders <- data.frame(id=c(3, 4), city=c("New York", "Atlanta"), stringsAsFactors=FALSE)

dbExecute(con, "DROP TABLE IF EXISTS orders")
dbExecute(con, "CREATE TABLE orders (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, city VARCHAR(255))")
dbxInsert(con, table="orders", records=orders[c("city")])

test_that("select works", {
  res <- dbxSelect(con, "SELECT * FROM orders ORDER BY id ASC")
  expect_equal(res, orders)
})

test_that("select order works", {
  res <- dbxSelect(con, "SELECT * FROM orders ORDER BY id DESC")
  expect_equal(res, reverse(orders))
})

test_that("select columns works", {
  res <- dbxSelect(con, "SELECT id FROM orders ORDER BY id ASC")
  expect_equal(res, orders[c("id")])
})

test_that("insert works", {
  dbxInsert(con, table="orders", records=new_orders[c("city")])
  res <- dbxSelect(con, "SELECT * FROM orders WHERE id > 2 ORDER BY id ASC")
  expect_equal(res, new_orders)
})

test_that("update works", {
  update_orders <- data.frame(id=c(3), city=c("LA"))
  dbxUpdate(con, table="orders", records=update_orders, where_cols=c("id"))
  res <- dbxSelect(con, "SELECT city FROM orders WHERE id = 3")
  expect_equal(c("LA"), res$city)
})

test_that("update missing column raises error", {
  update_orders <- data.frame(id=c(3), city=c("LA"))
  expect_error(dbxUpdate(con, table="orders", records=update_orders, where_cols=c("missing")), "where_cols not in records")
})

test_that("upsert works", {
  upsert_orders <- data.frame(id=c(3), city=c("Boston"))
  dbxUpsert(con, table="orders", records=upsert_orders, where_cols=c("id"))
  res <- dbxSelect(con, "SELECT city FROM orders WHERE id = 3")
  expect_equal(c("Boston"), res$city)

  upsert_orders <- data.frame(id=c(5), city=c("Chicago"))
  dbxUpsert(con, table="orders", records=upsert_orders, where_cols=c("id"))
  res <- dbxSelect(con, "SELECT city FROM orders WHERE id = 5")
  expect_equal(c("Chicago"), res$city)
  dbxDelete(con, table="orders", records=data.frame(id=c(5)))
})

test_that("upsert missing column raises error", {
  update_orders <- data.frame(id=c(3), city=c("LA"))
  expect_error(dbxUpsert(con, table="orders", records=update_orders, where_cols=c("missing")), "where_cols not in records")
})

test_that("delete works", {
  delete_orders <- data.frame(id=c(3))
  dbxDelete(con, table="orders", where=delete_orders)
  res <- dbxSelect(con, "SELECT * FROM orders ORDER BY id ASC")
  exp <- rbind(orders, new_orders)[c(1, 2, 4), ]
  rownames(exp) <- NULL
  expect_equal(res, exp)
})

test_that("delete all works", {
  dbxDelete(con, table="orders")
  res <- dbxSelect(con, "SELECT COUNT(*) AS count FROM orders")
  exp <- data.frame(count=0)
  expect_equal(res, exp)
})

test_that("connect with url works", {
  library(urltools)
  con2 <- dbxConnect("mysql://localhost/dbx_test")
  res <- dbxSelect(con2, "SELECT 1 AS hi")
  exp <- data.frame(hi=c(1))
  expect_equal(res, exp)
})