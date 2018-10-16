runDataTypeTests <- function(db, redshift=FALSE) {
  test_that("integer works", {
    dbxDelete(db, "events")

    events <- data.frame(counter=c(1, 2))
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res$counter, events$counter)
  })

  test_that("float works", {
    dbxDelete(db, "events")

    events <- data.frame(speed=c(1.2, 3.4))
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res$speed, events$speed, tolerance=0.000001)
  })

  test_that("decimal works", {
    dbxDelete(db, "events")

    events <- data.frame(distance=c(1.2, 3.4))
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res$distance, events$distance)
  })

  test_that("boolean works", {
    dbxDelete(db, "events")

    events <- data.frame(active=c(TRUE, FALSE))
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")

    if (isSQLite(db) || isRMariaDB(db)) {
      res$active <- res$active != 0
    } else if (isODBCPostgres(db)) {
      res$active <- res$active != "0"
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
    expect_equal(res$count, 1)
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
    expect_equal(res$count, 1)
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
    expect_equal(res$count, 1)
  })

  test_that("time zone is UTC", {
    # always utc
    skip_if(isSQLite(db) || isSQLServer(db))

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
    expect_equal(res$count, 1)
  })

  test_that("times with time zone work", {
    skip_if(!isPostgres(db))

    dbxDelete(db, "events")

    events <- data.frame(close_time=c("12:30:55", "16:59:59"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    # test returned time
    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")

    if (isODBCPostgres(db)) {
      expect_equal(res$close_time, paste0(events$close_time, "+00"))
    } else {
      expect_equal(res$close_time, events$close_time)
    }

    # test stored time
    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events WHERE close_time = '12:30:55'")
    expect_equal(res$count, 1)
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
    expect_equal(res$count, 1)
  })

  test_that("binary works", {
    skip_if(redshift || isODBCPostgres(db) || isSQLServer(db))

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
    skip_if(redshift || isODBCPostgres(db) || isSQLServer(db))

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

  test_that("ts uses observation values", {
    dbxDelete(db, "events")

    events <- data.frame(counter=ts(1:3, start=c(2018, 1), end=c(2018, 3), frequency=12))
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events ORDER BY id")
    expect_equal(res$counter, as.integer(events$counter))
  })

  # very important
  # shows typecasting is consistent
  test_that("can update what what just selected and get same result", {
    skip_if(isODBCPostgres(db) || isSQLServer(db))

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
  })
}
