context("postgresql")

skip_on_cran()

db <- dbxConnect(adapter="rpostgresql", dbname="dbx_test")

orders <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
new_orders <- data.frame(id=c(3, 4), city=c("New York", "Atlanta"), stringsAsFactors=FALSE)

dbExecute(db, "DROP TABLE IF EXISTS orders")
dbExecute(db, "CREATE TABLE orders (id SERIAL PRIMARY KEY, city text, other text)")
dbxInsert(db, "orders", orders[c("city")])

dbExecute(db, "DROP TABLE IF EXISTS events")
dbExecute(db, "CREATE TABLE events (id SERIAL PRIMARY KEY, created_on DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ, open_time TIME, close_time TIMETZ, active BOOLEAN, properties JSON, propertiesb JSONB, image BYTEA)")

test_that("select works", {
  res <- dbxSelect(db, "SELECT id, city FROM orders ORDER BY id ASC")
  expect_equal(res, orders)
})

test_that("select order works", {
  res <- dbxSelect(db, "SELECT id, city FROM orders ORDER BY id DESC")
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

  expect_equal(as.Date(NA), res$created_on)
  expect_equal(as.POSIXct(NA), res$updated_at)
  expect_equal(as.POSIXct(NA), res$deleted_at)
  expect_equal(NA, res$active)
})

test_that("insert works", {
  res <- dbxInsert(db, "orders", new_orders[c("city")])
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
  expect_equal(4, res$count)
})

test_that("delete one column works", {
  delete_orders <- data.frame(id=c(3))
  dbxDelete(db, "orders", where=delete_orders)
  res <- dbxSelect(db, "SELECT id, city FROM orders ORDER BY id ASC")
  exp <- rbind(orders, new_orders)[c(1, 2, 4), ]
  rownames(exp) <- NULL
  expect_equal(res, exp)
})

test_that("delete multiple columns works", {
  dbxDelete(db, "orders", where=orders)
  res <- dbxSelect(db, "SELECT id, city FROM orders ORDER BY id ASC")
  exp <- new_orders[c(2), ]
  rownames(exp) <- NULL
  expect_equal(res, exp)
})

test_that("delete all works", {
  dbxDelete(db, "orders")
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM orders")
  expect_equal(0, res$count)
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

test_that("jsonb works", {
  dbxDelete(db, "events")

  events <- data.frame(propertiesb=c('{"hello": "world"}'), stringsAsFactors=FALSE)
  res <- dbxInsert(db, "events", events)

  expect_equal(res$propertiesb, events$propertiesb)

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$propertiesb, events$propertiesb)
})

test_that("jsonlite with jsonb works", {
  dbxDelete(db, "events")

  events <- data.frame(propertiesb=c(jsonlite::toJSON(list(hello="world"))), stringsAsFactors=FALSE)
  res <- dbxInsert(db, "events", events)

  expect_equal(jsonlite::fromJSON(res$propertiesb), jsonlite::fromJSON(events$propertiesb))

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(jsonlite::fromJSON(res$propertiesb), jsonlite::fromJSON(events$propertiesb))
})

test_that("dates works", {
  dbxDelete(db, "events")

  events <- data.frame(created_on=as.Date(c("2018-01-01", "2018-01-02")))
  res <- dbxInsert(db, "events", events)

  expect_equal(res$created_on, events$created_on)

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$created_on, events$created_on)

  # dates always in UTC
  expect(all(format(res$created_on, "%Z") == "UTC"))
})

test_that("datetimes works", {
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

test_that("datetimes with time zones works", {
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

test_that("timestamp with time zone works", {
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

test_that("datetimes have precision", {
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
  expect_equal("UTC", dbxSelect(db, "SHOW timezone")$TimeZone)
})

test_that("datetimes with storage_tz works", {
  inTimeZone("America/Chicago", {
    db2 <- dbxConnect(adapter="rpostgresql", dbname="dbx_test", storage_tz="America/Chicago")
    dbxDelete(db2, "events")

    t1 <- as.POSIXct("2018-01-01 12:30:55", tz="America/New_York")
    t2 <- as.POSIXct("2018-01-01 16:59:59", tz="America/New_York")
    events <- data.frame(updated_at=c(t1, t2))
    dbxInsert(db2, "events", events)

    # test returned time
    res <- dbxSelect(db2, "SELECT * FROM events ORDER BY id")
    expect_equal(res$updated_at, events$updated_at)

    # test stored time
    res <- dbxSelect(db2, "SELECT COUNT(*) AS count FROM events WHERE updated_at = '2018-01-01 11:30:55'")
    expect_equal(1, res$count)

    dbxDisconnect(db2)
  })
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

test_that("times with time zone work", {
  dbxDelete(db, "events")

  events <- data.frame(close_time=c("12:30:55", "16:59:59"), stringsAsFactors=FALSE)
  res <- dbxInsert(db, "events", events)

  expect_equal(res$close_time, paste0(events$close_time, "+00"))

  # test returned time
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$close_time, paste0(events$close_time, "+00"))

  # test stored time
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE close_time = '12:30:55'")
  expect_equal(1, res$count)
})

test_that("hms with times work", {
  dbxDelete(db, "events")

  events <- data.frame(open_time=c(hms::as.hms("12:30:55"), hms::as.hms("16:59:59")), stringsAsFactors=FALSE)
  res <- dbxInsert(db, "events", events)

  expect_equal(res$open_time, as.character(events$open_time))

  # test returned time
  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(res$open_time, as.character(events$open_time))

  # test stored time
  res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE open_time = '12:30:55'")
  expect_equal(1, res$count)
})

test_that("cast_times true works", {
  db2 <- dbxConnect(adapter="rpostgresql", dbname="dbx_test", cast_times=TRUE)

  dbxDelete(db2, "events")

  events <- data.frame(open_time=c(hms::as.hms("12:30:55"), hms::as.hms("16:59:59")), stringsAsFactors=FALSE)
  res <- dbxInsert(db2, "events", events)

  expect_equal(res$open_time, events$open_time)

  # test returned time
  res <- dbxSelect(db2, "SELECT * FROM events ORDER BY id")
  expect_equal(res$open_time, events$open_time)

  # test stored time
  res <- dbxSelect(db2, "SELECT COUNT(*) AS count FROM events WHERE open_time = '12:30:55'")
  expect_equal(1, res$count)

  dbxDisconnect(db2)
})

test_that("cast_times false works", {
  db2 <- dbxConnect(adapter="rpostgresql", dbname="dbx_test", cast_times=FALSE)

  dbxDelete(db2, "events")

  events <- data.frame(open_time=c(hms::as.hms("12:30:55"), hms::as.hms("16:59:59")), stringsAsFactors=FALSE)
  res <- dbxInsert(db2, "events", events)

  expect_equal(res$open_time, as.character(events$open_time))

  # test returned time
  res <- dbxSelect(db2, "SELECT * FROM events ORDER BY id")
  expect_equal(res$open_time, as.character(events$open_time))

  # test stored time
  res <- dbxSelect(db2, "SELECT COUNT(*) AS count FROM events WHERE open_time = '12:30:55'")
  expect_equal(1, res$count)

  dbxDisconnect(db2)
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

  expect_equal(blob::as.blob(res$image), events$image)

  res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
  expect_equal(blob::as.blob(res$image), events$image)
})

test_that("cast_blobs true works", {
  db2 <- dbxConnect(adapter="rpostgresql", dbname="dbx_test", cast_blobs=TRUE)

  dbxDelete(db2, "events")

  images <- list(1:3, 4:6)
  serialized_images <- lapply(images, function(x) { serialize(x, NULL) })

  events <- data.frame(image=blob::as.blob(serialized_images))
  res <- dbxInsert(db2, "events", events)

  expect_equal(res$image, events$image)

  res <- dbxSelect(db2, "SELECT * FROM events ORDER BY id")
  expect_equal(res$image, events$image)

  dbxDisconnect(db2)
})

test_that("cast_blobs false works", {
  db2 <- dbxConnect(adapter="rpostgresql", dbname="dbx_test", cast_blobs=FALSE)

  dbxDelete(db2, "events")

  images <- list(1:3, 4:6)
  serialized_images <- lapply(images, function(x) { serialize(x, NULL) })

  events <- data.frame(image=blob::as.blob(serialized_images))
  res <- dbxInsert(db2, "events", events)

  expect_equal(res$image, lapply(events$image, as.raw))

  res <- dbxSelect(db2, "SELECT * FROM events ORDER BY id")
  expect_equal(res$image, lapply(events$image, as.raw))

  dbxDisconnect(db2)
})

dbxDisconnect(db)
