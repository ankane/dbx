context("mysql")

skip_on_cran()

db <- dbxConnect(adapter="mysql", dbname="dbx_test")

orders <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
new_orders <- data.frame(id=c(3, 4), city=c("New York", "Atlanta"), stringsAsFactors=FALSE)

dbExecute(db, "DROP TABLE IF EXISTS orders")
dbExecute(db, "CREATE TABLE orders (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, city VARCHAR(255))")
dbxInsert(db, "orders", orders[c("city")])

dbExecute(db, "DROP TABLE IF EXISTS events")
dbExecute(db, "CREATE TABLE events (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, created_on DATE, updated_at DATETIME(6), deleted_at TIMESTAMP(6), active BOOLEAN, properties JSON)")

test_that("select works", {
  res <- dbxSelect(db, "SELECT * FROM orders ORDER BY id ASC")
  expect_equal(res, orders)
})

test_that("select order works", {
  res <- dbxSelect(db, "SELECT * FROM orders ORDER BY id DESC")
  expect_equal(res, reverse(orders))
})

test_that("select columns works", {
  res <- dbxSelect(db, "SELECT id FROM orders ORDER BY id ASC")
  expect_equal(res, orders[c("id")])
})

test_that("insert works", {
  dbxInsert(db, "orders", new_orders[c("city")])
  res <- dbxSelect(db, "SELECT * FROM orders WHERE id > 2 ORDER BY id ASC")
  expect_equal(res, new_orders)
})

test_that("update works", {
  update_orders <- data.frame(id=c(3), city=c("LA"))
  dbxUpdate(db, "orders", update_orders, where_cols=c("id"))
  res <- dbxSelect(db, "SELECT city FROM orders WHERE id = 3")
  expect_equal(c("LA"), res$city)
})

test_that("update missing column raises error", {
  update_orders <- data.frame(id=c(3), city=c("LA"))
  expect_error(dbxUpdate(db, "orders", update_orders, where_cols=c("missing")), "where_cols not in records")
})

test_that("upsert works", {
  upsert_orders <- data.frame(id=c(3, 5), city=c("Boston", "Chicago"))
  dbxUpsert(db, "orders", upsert_orders, where_cols=c("id"))
  res <- dbxSelect(db, "SELECT city FROM orders WHERE id IN (3, 5)")
  expect_equal(c("Boston", "Chicago"), res$city)
  dbxDelete(db, "orders", data.frame(id=c(5)))
})

test_that("upsert missing column raises error", {
  update_orders <- data.frame(id=c(3), city=c("LA"))
  expect_error(dbxUpsert(db, "orders", update_orders, where_cols=c("missing")), "where_cols not in records")
})

test_that("delete empty does not delete rows", {
  delete_orders <- data.frame(id=c())
  dbxDelete(db, "orders", where=delete_orders)
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM orders")
  exp <- data.frame(count=4)
  expect_equal(res, exp)
})

test_that("delete one column works", {
  delete_orders <- data.frame(id=c(3))
  dbxDelete(db, "orders", where=delete_orders)
  res <- dbxSelect(db, "SELECT * FROM orders ORDER BY id ASC")
  exp <- rbind(orders, new_orders)[c(1, 2, 4), ]
  rownames(exp) <- NULL
  expect_equal(res, exp)
})

test_that("delete multiple columns works", {
  dbxDelete(db, "orders", where=orders)
  res <- dbxSelect(db, "SELECT * FROM orders ORDER BY id ASC")
  exp <- new_orders[c(2), ]
  rownames(exp) <- NULL
  expect_equal(res, exp)
})

test_that("delete all works", {
  dbxDelete(db, "orders")
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM orders")
  exp <- data.frame(count=0)
  expect_equal(res, exp)
})

test_that("insert batch size works", {
  res <- dbxInsert(db, "orders", orders, batch_size=1)
  expect_equal(res, orders)
})

test_that("empty insert works", {
  empty_orders <- data.frame()
  res <- dbxInsert(db, "orders", empty_orders)
  expect_equal(res, empty_orders)
})

test_that("boolean works", {
  dbxDelete(db, "events")

  events <- data.frame(active=c(TRUE, FALSE))
  res <- dbxInsert(db, "events", events)

  expect_equal(res$active, events$active)

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$active, events$active)
})

test_that("json works", {
  dbxDelete(db, "events")

  events <- data.frame(properties=c('{"hello": "world"}'), stringsAsFactors=FALSE)
  res <- dbxInsert(db, "events", events)

  expect_equal(res$properties, events$properties)

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$properties, events$properties)
})

test_that("dates works", {
  dbxDelete(db, "events")

  events <- data.frame(created_on=as.Date(c("2018-01-01", "2018-01-02")))
  res <- dbxInsert(db, "events", events)

  expect_equal(res$created_on, events$created_on)

  # dates returned as character vectors
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$created_on, events$created_on)

  # test typecast
  expect_equal(as.Date(res$created_on), events$created_on)
})

test_that("times works", {
  dbxDelete(db, "events")

  t1 <- as.POSIXct("2018-01-01 12:30:55")
  t2 <- as.POSIXct("2018-01-01 16:59:59")
  events <- data.frame(updated_at=c(t1, t2))
  res <- dbxInsert(db, "events", events)

  expect_equal(res$updated_at, events$updated_at)

  # test returned time
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$updated_at, events$updated_at)

  # test stored time
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE updated_at = '2018-01-01 20:30:55'")
  expect_equal(1, res$count)
})

test_that("time zones works", {
  dbxDelete(db, "events")

  t1 <- as.POSIXct("2018-01-01 12:30:55", tz="America/New_York")
  t2 <- as.POSIXct("2018-01-01 16:59:59", tz="America/New_York")
  events <- data.frame(updated_at=c(t1, t2))
  dbxInsert(db, "events", events)

  # test returned time
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$updated_at, events$updated_at)

  # test stored time
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE updated_at = '2018-01-01 17:30:55'")
  expect_equal(1, res$count)
})

test_that("timestamp works", {
  dbxDelete(db, "events")

  t1 <- as.POSIXct("2018-01-01 12:30:55", tz="America/New_York")
  t2 <- as.POSIXct("2018-01-01 16:59:59", tz="America/New_York")
  events <- data.frame(deleted_at=c(t1, t2))
  dbxInsert(db, "events", events)

  # test returned time
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$deleted_at, events$deleted_at)

  # test stored time
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE deleted_at = '2018-01-01 17:30:55'")
  expect_equal(1, res$count)
})

test_that("timestamps have precision", {
  dbxDelete(db, "events")

  t1 <- as.POSIXct("2018-01-01 12:30:55.123456")
  events <- data.frame(updated_at=c(t1))
  dbxInsert(db, "events", events)

  # test returned time
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$updated_at, events$updated_at)

  # test stored time
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE updated_at = '2018-01-01 20:30:55.123456'")
  expect_equal(1, res$count)
})

test_that("time zone is UTC", {
  expect_equal("+00:00", dbxSelect(db, "SELECT @@session.time_zone")$`@@session.time_zone`)
})

test_that("connect with url works", {
  con2 <- dbxConnect("mysql://localhost/dbx_test")
  res <- dbxSelect(con2, "SELECT 1 AS hi")
  dbxDisconnect(con2)
  exp <- data.frame(hi=c(1))
  expect_equal(res, exp)
})

dbxDisconnect(db)
