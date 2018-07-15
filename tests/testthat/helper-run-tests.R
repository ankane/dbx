isRPostgreSQL <- function(conn) {
  inherits(conn, "PostgreSQLConnection")
}

isRPostgres <- function(conn) {
  inherits(conn, "PqConnection")
}

isPostgres <- function(conn) {
  isRPostgreSQL(conn) || isRPostgres(conn)
}

isRMySQL <- function(conn) {
  inherits(conn, "MySQLConnection")
}

isMySQL <- function(conn) {
  isRMySQL(conn) || isRMariaDB(conn)
}

isRMariaDB <- function(conn) {
  inherits(conn, "MariaDBConnection")
}

isSQLite <- function(conn) {
  inherits(conn, "SQLiteConnection")
}

runTests <- function(db, redshift=FALSE) {
  orders <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
  new_orders <- data.frame(id=c(3, 4), city=c("New York", "Atlanta"), stringsAsFactors=FALSE)

  dbxInsert(db, "orders", orders)

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

    dbxInsert(db, "events", data.frame(properties=NA))
    res <- dbxSelect(db, "SELECT * FROM events")

    if (isSQLite(db)) {
      expect_equal(as.numeric(NA), res$created_on)
      expect_equal(as.numeric(NA), res$updated_at)
      expect_equal(as.numeric(NA), res$active)
    } else {
      expect_equal(as.Date(NA), res$created_on)
      expect_equal(as.POSIXct(NA), res$updated_at)
      expect_equal(as.POSIXct(NA), res$deleted_at)
      if (isRMariaDB(db)) {
        expect_equal(as.numeric(NA), res$active)
      } else {
        expect_equal(NA, res$active)
      }
    }
  })

  test_that("insert works", {
    dbxInsert(db, "orders", new_orders)

    res <- dbxSelect(db, "SELECT * FROM orders WHERE id > 2 ORDER BY id")
    expect_equal(res$city, new_orders$city)
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
    skip_if(isSQLite(db) || redshift)

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

  test_that("empty insert works", {
    dbxInsert(db, "events", data.frame())
    expect(TRUE)
  })

  test_that("empty update works", {
    dbxUpdate(db, "events", data.frame(id = as.numeric(), active = as.logical()), where_cols=c("id"))
    expect(TRUE)
  })

  test_that("empty upsert works", {
    skip_if(!isPostgres(db) || redshift)

    dbxUpsert(db, "events", data.frame(id = as.numeric(), active = as.logical()), where_cols=c("id"))
    expect(TRUE)
  })

  test_that("insert returning works", {
    skip_if(!isPostgres(db) || redshift)

    res <- dbxInsert(db, "orders", orders[c("city")], returning=c("id", "city"))
    expect_equal(res$id, c(1, 2))
    expect_equal(res$city, orders$city)

    res <- dbxInsert(db, "orders", orders[c("city")], returning="*")
    expect_equal(res$id, c(3, 4))
    expect_equal(res$city, orders$city)
  })

  test_that("insert batch size works", {
    dbxDelete(db, "orders")
    dbxInsert(db, "orders", orders, batch_size=1)

    res <- dbxSelect(db, "SELECT id, city FROM orders")
    expect_equal(res, orders)
  })

  test_that("boolean works", {
    dbxDelete(db, "events")

    events <- data.frame(active=c(TRUE, FALSE))
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")

    if (isSQLite(db) || isRMariaDB(db)) {
      res$active <- res$active != 0
    }

    expect_equal(res$active, events$active)
  })

  test_that("json works", {
    skip_if(isRMariaDB(db))

    dbxDelete(db, "events")

    events <- data.frame(properties=c('{"hello": "world"}'), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res$properties, events$properties)
  })

  test_that("jsonb works", {
    skip_if(!isPostgres(db))

    dbxDelete(db, "events")

    events <- data.frame(propertiesb=c('{"hello": "world"}'), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res$propertiesb, events$propertiesb)
  })

  test_that("jsonlite with jsonb works", {
    skip_if(!isPostgres(db))

    dbxDelete(db, "events")

    events <- data.frame(propertiesb=c(jsonlite::toJSON(list(hello="world"))), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(jsonlite::fromJSON(res$propertiesb), jsonlite::fromJSON(events$propertiesb))
  })

  test_that("dates works", {
    dbxDelete(db, "events")

    events <- data.frame(created_on=as.Date(c("2018-01-01", "2018-01-02")))
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")

    if (isSQLite(db)) {
      res$created_on <- as.Date(res$created_on)
    }

    expect_equal(res$created_on, events$created_on)

    # dates always in UTC
    expect(all(format(res$created_on, "%Z") == "UTC"))
  })

  test_that("datetimes works", {
    dbxDelete(db, "events")

    t1 <- as.POSIXct("2018-01-01 12:30:55")
    t2 <- as.POSIXct("2018-01-01 16:59:59")
    events <- data.frame(updated_at=c(t1, t2))
    dbxInsert(db, "events", events)

    # test returned time
    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")

    if (isSQLite(db)) {
      res$updated_at <- as.POSIXct(res$updated_at, tz="Etc/UTC")
      attr(res$updated_at, "tzone") <- Sys.timezone()
    }

    expect_equal(res$updated_at, events$updated_at)

    # test stored time
    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE updated_at = '2018-01-01 20:30:55.000000'")
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

    if (isSQLite(db)) {
      res$updated_at <- as.POSIXct(res$updated_at, tz="Etc/UTC")
      attr(res$updated_at, "tzone") <- Sys.timezone()
    }

    expect_equal(res$updated_at, events$updated_at)

    # test stored time
    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE updated_at = '2018-01-01 17:30:55.000000'")
    expect_equal(1, res$count)
  })

  test_that("timestamp with time zone works", {
    skip_if(isSQLite(db))

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

    if (isSQLite(db)) {
      res$updated_at <- as.POSIXct(res$updated_at, tz="Etc/UTC")
      attr(res$updated_at, "tzone") <- Sys.timezone()
    }

    expect_equal(res$updated_at, events$updated_at)

    # test stored time
    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE updated_at = '2018-01-01 20:30:55.123456'")
    expect_equal(1, res$count)
  })

  test_that("time zone is UTC", {
    # always utc
    skip_if(isSQLite(db))

    if (isPostgres(db)) {
      expect_equal("UTC", dbxSelect(db, "SHOW timezone")$TimeZone)
    } else {
      expect_equal("+00:00", dbxSelect(db, "SELECT @@session.time_zone")$`@@session.time_zone`)
    }
  })

  test_that("times work", {
    dbxDelete(db, "events")

    events <- data.frame(open_time=c("12:30:55", "16:59:59"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    # test returned time
    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res$open_time, events$open_time)

    # test stored time
    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE open_time = '12:30:55'")
    expect_equal(1, res$count)
  })

  test_that("times with time zone work", {
    skip_if(!isPostgres(db))

    dbxDelete(db, "events")

    events <- data.frame(close_time=c("12:30:55", "16:59:59"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    # test returned time
    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res$close_time, events$close_time)

    # test stored time
    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE close_time = '12:30:55'")
    expect_equal(1, res$count)
  })

  test_that("hms with times work", {
    dbxDelete(db, "events")

    events <- data.frame(open_time=c(hms::as.hms("12:30:55"), hms::as.hms("16:59:59")), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    # test returned time
    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res$open_time, as.character(events$open_time))

    # test stored time
    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE open_time = '12:30:55'")
    expect_equal(1, res$count)
  })

  test_that("binary works", {
    skip_if(redshift)

    dbxDelete(db, "events")

    images <- list(1:3, 4:6)
    serialized_images <- lapply(images, function(x) { serialize(x, NULL) })

    events <- data.frame(image=I(serialized_images))
    dbxInsert(db, "events", events)

    if (isRMySQL(db)) {
      res <- dbxSelect(db, "SELECT hex(image) AS image FROM events ORDER BY id")
      res$image <- lapply(res$image, hexToRaw)
    } else {
      res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    }

    expect_equal(lapply(res$image, unserialize), images)
  })

  test_that("blob with binary works", {
    skip_if(redshift)

    dbxDelete(db, "events")

    images <- list(1:3, 4:6)
    serialized_images <- lapply(images, function(x) { serialize(x, NULL) })

    events <- data.frame(image=blob::as.blob(serialized_images))
    dbxInsert(db, "events", events)

    if (isRMySQL(db)) {
      res <- dbxSelect(db, "SELECT hex(image) AS image FROM events ORDER BY id")
      res$image <- lapply(res$image, hexToRaw)
    } else {
      res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    }

    expect_equal(blob::as.blob(res$image), events$image)
  })

  # very important
  # shows typecasting is consistent
  test_that("can update what what just selected and get same result", {
    dbxDelete(db, "events")

    df <- data.frame(
      active=c(TRUE, FALSE),
      created_on=as.Date(c("2018-01-01", "2018-02-01")),
      updated_at=as.POSIXct(c("2018-01-01 12:30:55", "2018-01-01 16:59:59")),
      open_time=c("09:30:55", "13:59:59"),
      properties=c('{"hello": "world"}', '{"hello": "r"}')
    )
    dbxInsert(db, "events", df)
    all <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    dbxUpdate(db, "events", all, where_cols=c("id"))
    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res, all)
    expect_equal(class(res$image), "list")
    expect_equal(class(res$image[[1]]), "raw")
  })

  dbxDisconnect(db)
}
