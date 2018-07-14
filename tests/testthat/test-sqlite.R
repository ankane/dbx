context("sqlite")

db <- dbxConnect(adapter="sqlite", dbname=":memory:")

orders <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
new_orders <- data.frame(id=c(3, 4), city=c("New York", "Atlanta"), stringsAsFactors=FALSE)

dbExecute(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, city VARCHAR(255))")
dbxInsert(db, "orders", orders)

dbExecute(db, "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, created_on DATE, updated_at DATETIME, open_time VARCHAR(255), active BOOLEAN, properties TEXT, image BLOB)")

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

test_that("empty select works", {
  dbxDelete(db, "events")
  res <- dbxSelect(db, "SELECT * FROM events")
  expect_equal(0, nrow(res))
})

test_that("missing select returns NA", {
  dbxDelete(db, "events")

  dbxInsert(db, "events", data.frame(id=1))
  res <- dbxSelect(db, "SELECT * FROM events")

  expect_equal(as.numeric(NA), res$created_on)
  expect_equal(as.numeric(NA), res$updated_at)
  expect_equal(as.numeric(NA), res$active)
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

  # typecasting not supported yet
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$active == 1, events$active)
})

test_that("json as text works", {
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

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$created_on, format(events$created_on))

  # test typecast
  expect_equal(as.Date(res$created_on), events$created_on)
})

test_that("datetimes works", {
  dbxDelete(db, "events")

  t1 <- as.POSIXct("2018-01-01 12:30:55")
  t2 <- as.POSIXct("2018-01-01 16:59:59")
  events <- data.frame(updated_at=c(t1, t2))
  res <- dbxInsert(db, "events", events)

  expect_equal(res$updated_at, events$updated_at)

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$updated_at, format(events$updated_at, tz="UTC", "%Y-%m-%d %H:%M:%OS6"))

  # test typecast
  col <- as.POSIXct(res$updated_at, tz="Etc/UTC")
  attr(col, "tzone") <- Sys.timezone()
  expect_equal(col, events$updated_at)
})

test_that("datetimes have precision", {
  dbxDelete(db, "events")

  t1 <- as.POSIXct("2018-01-01 12:30:55.123456")
  events <- data.frame(updated_at=c(t1))
  dbxInsert(db, "events", events)

  # test returned time
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$updated_at, format(events$updated_at, tz="UTC", "%Y-%m-%d %H:%M:%OS6"))

  # test stored time
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE updated_at = '2018-01-01 20:30:55.123456'")
  expect_equal(1, res$count)
})

test_that("times work", {
  dbxDelete(db, "events")

  events <- data.frame(open_time=c("12:30:55", "16:59:59"), stringsAsFactors=FALSE)
  res <- dbxInsert(db, "events", events)

  expect_equal(res$open_time, events$open_time)

  # test returned time
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$open_time, events$open_time)

  # test stored time
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE open_time = '12:30:55'")
  expect_equal(1, res$count)
})

test_that("hms with times work", {
  dbxDelete(db, "events")

  events <- data.frame(open_time=c(hms::as.hms("12:30:55"), hms::as.hms("16:59:59")), stringsAsFactors=FALSE)
  res <- dbxInsert(db, "events", events)

  expect_equal(res$open_time, events$open_time)

  # test returned time
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$open_time, as.character(events$open_time))

  # test stored time
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE open_time = '12:30:55'")
  expect_equal(1, res$count)
})

test_that("binary works", {
  dbxDelete(db, "events")

  images <- list(1:3, 4:6)
  serialized_images <- lapply(images, function(x) { serialize(x, NULL) })

  events <- data.frame(image=I(serialized_images))
  res <- dbxInsert(db, "events", events)

  expect_equal(lapply(res$image, unserialize), images)

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(lapply(res$image, unserialize), images)
})

test_that("blob with binary works", {
  dbxDelete(db, "events")

  images <- list(1:3, 4:6)
  serialized_images <- lapply(images, function(x) { serialize(x, NULL) })

  events <- data.frame(image=blob::as.blob(serialized_images))
  res <- dbxInsert(db, "events", events)

  expect_equal(res$image, events$image)

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(blob::as.blob(res$image), events$image)
})

# test_that("cast_binary works", {
#   db2 <- dbxConnect(adapter="sqlite", dbname=":memory:", cast_binary="blob")
#   dbExecute(db2, "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, created_on DATE, updated_at DATETIME, active BOOLEAN, image BLOB)")

#   images <- list(1:3, 4:6)
#   serialized_images <- lapply(images, function(x) { serialize(x, NULL) })

#   events <- data.frame(image=blob::as.blob(serialized_images))
#   res <- dbxInsert(db2, "events", events)

#   expect_equal(res$image, events$image)

#   res <- dbxSelect(db2, "SELECT * FROM events ORDER BY id")
#   expect_equal(res$image, events$image)

#   dbxDisconnect(db2)
# })

test_that("connect with url works", {
  con2 <- dbxConnect(url="sqlite:///:memory:")
  res <- dbxSelect(con2, "SELECT 1 AS hi")
  dbxDisconnect(con2)
  exp <- data.frame(hi=c(1))
  expect_equal(res, exp)
})

dbxDisconnect(db)
