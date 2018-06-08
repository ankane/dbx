context("sqlite")

con <- dbxConnect(adapter="sqlite", dbname=":memory:")

orders <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
new_orders <- data.frame(id=c(3, 4), city=c("New York", "Atlanta"), stringsAsFactors=FALSE)

dbExecute(con, "CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, city VARCHAR(255))")
dbxInsert(con, "orders", orders)

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
  dbxInsert(con, "orders", new_orders[c("city")])
  res <- dbxSelect(con, "SELECT * FROM orders WHERE id > 2 ORDER BY id ASC")
  expect_equal(res, new_orders)
})

test_that("update works", {
  update_orders <- data.frame(id=c(3), city=c("LA"))
  dbxUpdate(con, "orders", update_orders, where_cols=c("id"))
  res <- dbxSelect(con, "SELECT city FROM orders WHERE id = 3")
  expect_equal(c("LA"), res$city)
})

test_that("update missing column raises error", {
  update_orders <- data.frame(id=c(3), city=c("LA"))
  expect_error(dbxUpdate(con, "orders", update_orders, where_cols=c("missing")), "where_cols not in records")
})

test_that("delete empty does not delete rows", {
  delete_orders <- data.frame(id=c())
  dbxDelete(con, "orders", where=delete_orders)
  res <- dbxSelect(con, "SELECT COUNT(*) AS count FROM orders")
  exp <- data.frame(count=4)
  expect_equal(res, exp)
})

test_that("delete one column works", {
  delete_orders <- data.frame(id=c(3))
  dbxDelete(con, "orders", where=delete_orders)
  res <- dbxSelect(con, "SELECT * FROM orders ORDER BY id ASC")
  exp <- rbind(orders, new_orders)[c(1, 2, 4), ]
  rownames(exp) <- NULL
  expect_equal(res, exp)
})

test_that("delete multiple columns works", {
  dbxDelete(con, "orders", where=orders)
  res <- dbxSelect(con, "SELECT * FROM orders ORDER BY id ASC")
  exp <- new_orders[c(2), ]
  rownames(exp) <- NULL
  expect_equal(res, exp)
})

test_that("delete all works", {
  dbxDelete(con, "orders")
  res <- dbxSelect(con, "SELECT COUNT(*) AS count FROM orders")
  exp <- data.frame(count=0)
  expect_equal(res, exp)
})

test_that("connect with url works", {
  library(urltools)
  con2 <- dbxConnect(url="sqlite:///:memory:")
  res <- dbxSelect(con2, "SELECT 1 AS hi")
  exp <- data.frame(hi=c(1))
  expect_equal(res, exp)
})
