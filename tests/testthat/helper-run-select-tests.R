runSelectTests <- function(db) {
  test_that("select works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT id, city FROM events ORDER BY id ASC")
    expect_equal(res, events)
  })

  test_that("select order works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT id, city FROM events ORDER BY id DESC")
    expect_equal(res, reverse(events))
  })

  test_that("select columns works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT id FROM events ORDER BY id ASC")
    expect_equal(res, events[c("id")])
  })

  test_that("empty select works", {
    res <- dbxSelect(db, "SELECT * FROM events")
    expect_equal(nrow(res), 0)
  })

  test_that("empty result", {
    res <- dbxSelect(db, "SELECT * FROM events")

    # numeric
    expect_identical(res$counter, as.integer())
    expect_identical(res$speed, as.numeric())
    expect_identical(res$distance, as.numeric())

    # dates and times
    if (isSQLite(db)) {
      # empty datetimes are numeric
      expect_identical(res$created_on, as.numeric())
      expect_identical(res$updated_at, as.numeric())
    } else {
      expect_identical(res$created_on, as.Date(as.character()))

      # not identical due to empty tzone attribute
      expect_equal(res$updated_at, as.POSIXct(as.character()))
      expect_equal(res$deleted_at, as.POSIXct(as.character()))

      expect_identical(res$open_time, as.character())
    }

    # json
    expect_identical(res$properties, as.character())

    # booleans
    if (isRMariaDB(db)) {
      # until proper typecasting
      expect_identical(res$active, as.integer())
    } else if (isSQLite(db)) {
      # until proper typecasting
      expect_identical(res$active, as.numeric())
    } else if (isODBCPostgres(db)) {
      expect_identical(res$active, as.character())
    } else {
      expect_identical(res$active, as.logical())
    }

    # binary
    if (isRMySQL(db)) {
      # no way to tell text and blobs apart
      expect_identical(class(res$image), "character")
    } else {
      expect_identical(class(res$image), "list")
    }
  })

  test_that("empty cells", {
    dbxInsert(db, "events", data.frame(counter=as.integer(NA)))
    res <- dbxSelect(db, "SELECT * FROM events")

    # numeric
    expect_identical(res$counter, as.integer(NA))
    expect_identical(res$speed, as.numeric(NA))
    expect_identical(res$distance, as.numeric(NA))

    # dates and times
    if (isSQLite(db)) {
      # empty datetimes are numeric
      expect_identical(res$created_on, as.numeric(NA))
      expect_identical(res$updated_at, as.numeric(NA))
    } else {
      expect_identical(res$created_on, as.Date(NA))

      # not identical due to empty tzone attribute
      expect_equal(res$updated_at, as.POSIXct(NA))
      expect_equal(res$deleted_at, as.POSIXct(NA))

      expect_identical(res$open_time, as.character(NA))
    }

    # json
    expect_identical(res$properties, as.character(NA))

    # booleans
    if (isRMariaDB(db)) {
      # until proper typecasting
      expect_identical(res$active, as.integer(NA))
    } else if (isSQLite(db)) {
      # until proper typecasting
      expect_identical(res$active, as.numeric(NA))
    } else if (isODBCPostgres(db)) {
      expect_identical(res$active, as.character(NA))
    } else {
      expect_identical(res$active, NA)
    }

    # binary
    if (isRMySQL(db)) {
      # RMySQL cannot return binary data
      expect_identical(class(res$image), "character")
    } else {
      expect_identical(class(res$image), "list")
      expect_null(res$image[[1]])
    }
  })

  test_that("string (character) params works", {
    events <- data.frame(city=c("Boston", "San Francisco"))
    dbxInsert(db, "events", events)

    params <- list("Boston")
    sql <- "SELECT COUNT(*) AS count FROM events WHERE city = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("string (complex) params works", {
    events <- data.frame(city=c(1+4i, 2+2i))
    dbxInsert(db, "events", events)

    params <- list(1+4i)
    sql <- "SELECT COUNT(*) AS count FROM events WHERE city = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("integer params works", {
    events <- data.frame(counter=c(1L, 2L))
    dbxInsert(db, "events", events)

    params <- list(1)
    sql <- "SELECT COUNT(*) AS count FROM events WHERE counter = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("float (double) params works", {
    events <- data.frame(speed=c(1.2, 3.4))
    dbxInsert(db, "events", events)

    params <- list(1.3)
    sql <- "SELECT COUNT(*) AS count FROM events WHERE speed < ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("decimal (double) params works", {
    events <- data.frame(distance=c(1.2, 3.4))
    dbxInsert(db, "events", events)

    params <- list(1.2)
    sql <- "SELECT COUNT(*) AS count FROM events WHERE distance = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("boolean (logical) params works", {
    events <- data.frame(active=c(TRUE, FALSE))
    dbxInsert(db, "events", events)

    params <- list(TRUE)
    sql <- "SELECT COUNT(*) AS count FROM events WHERE active = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("dates params works", {
    events <- data.frame(created_on=as.Date(c("2018-01-01", "2018-01-02")))
    dbxInsert(db, "events", events)

    params <- list(as.Date(c("2018-01-01")))
    sql <- "SELECT COUNT(*) AS count FROM events WHERE created_on = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("datetimes params works", {
    t1 <- as.POSIXct("2018-01-01 12:30:55")
    t2 <- as.POSIXct("2018-01-01 16:59:59")
    events <- data.frame(updated_at=c(t1, t2))
    dbxInsert(db, "events", events)

    params <- list(t1)
    sql <- "SELECT COUNT(*) AS count FROM events WHERE updated_at = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("times work", {
    events <- data.frame(open_time=c("12:30:55", "16:59:59"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    params <- list(c("12:30:55"))
    sql <- "SELECT COUNT(*) AS count FROM events WHERE open_time = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("factor params works", {
    events <- data.frame(city=c("Boston", "San Francisco"))
    dbxInsert(db, "events", events)

    params <- list(factor("Boston"))
    sql <- "SELECT COUNT(*) AS count FROM events WHERE city = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("multiple params works", {
    events <- data.frame(counter=c(1, 2), city=c("Boston", "Chicago"))
    dbxInsert(db, "events", events)

    params <- list(1, "Chicago")
    sql <- "SELECT COUNT(*) AS count FROM events WHERE counter = ? OR city = ?"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 2)
  })

  test_that("vector params works", {
    events <- data.frame(counter=c(1, 2))
    dbxInsert(db, "events", events)

    params <- list(events$counter)
    sql <- "SELECT COUNT(*) AS count FROM events WHERE counter IN (?)"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 2)
  })

  test_that("vector string params works", {
    events <- data.frame(city=c("San Francisco", "Boston"))
    dbxInsert(db, "events", events)

    params <- list(events$city)
    sql <- "SELECT COUNT(*) AS count FROM events WHERE city IN (?)"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 2)
  })

  test_that("empty vector params works", {
    # due to https://github.com/r-dbi/RPostgres/pull/370
    skip_if(isRPostgres(db))

    events <- data.frame(counter=c(1, 2))
    dbxInsert(db, "events", events)

    params <- list(c())
    sql <- "SELECT COUNT(*) AS count FROM events WHERE counter IN (?)"
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 0)
  })

  test_that("wrong params", {
    events <- data.frame(counter=c(1, 2))
    dbxInsert(db, "events", events)

    params <- list(1)
    sql <- "SELECT COUNT(*) AS count FROM events WHERE id = ? AND counter = ?"
    expect_error(dbxSelect(db, sql, params=params), "Wrong number of params")
  })
}
