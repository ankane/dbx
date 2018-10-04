runParamsTests <- function(db) {
  test_that("integer params works", {
    dbxDelete(db, "events")

    events <- data.frame(counter=c(1, 2))
    dbxInsert(db, "events", events)

    params <- list(1)
    sql <- bindSQL(db, "SELECT COUNT(*) AS count FROM events WHERE counter = $1")
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("float params works", {
    dbxDelete(db, "events")

    events <- data.frame(speed=c(1.2, 3.4))
    dbxInsert(db, "events", events)

    params <- list(1.3)
    sql <- bindSQL(db, "SELECT COUNT(*) AS count FROM events WHERE speed < $1")
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("decimal params works", {
    dbxDelete(db, "events")

    events <- data.frame(distance=c(1.2, 3.4))
    dbxInsert(db, "events", events)

    params <- list(1.2)
    sql <- bindSQL(db, "SELECT COUNT(*) AS count FROM events WHERE distance = $1")
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("boolean params works", {
    dbxDelete(db, "events")

    events <- data.frame(active=c(TRUE, FALSE))
    dbxInsert(db, "events", events)

    params <- list(TRUE)
    sql <- bindSQL(db, "SELECT COUNT(*) AS count FROM events WHERE active = $1")
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("dates params works", {
    dbxDelete(db, "events")

    events <- data.frame(created_on=as.Date(c("2018-01-01", "2018-01-02")))
    dbxInsert(db, "events", events)

    params <- list(as.Date(c("2018-01-01")))
    sql <- bindSQL(db, "SELECT COUNT(*) AS count FROM events WHERE created_on = $1")
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("datetimes params works", {
    dbxDelete(db, "events")

    t1 <- as.POSIXct("2018-01-01 12:30:55")
    t2 <- as.POSIXct("2018-01-01 16:59:59")
    events <- data.frame(updated_at=c(t1, t2))
    dbxInsert(db, "events", events)

    params <- list(t1)
    sql <- bindSQL(db, "SELECT COUNT(*) AS count FROM events WHERE updated_at = $1")
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })

  test_that("times work", {
    dbxDelete(db, "events")

    events <- data.frame(open_time=c("12:30:55", "16:59:59"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    params <- list(c("12:30:55"))
    sql <- bindSQL(db, "SELECT COUNT(*) AS count FROM events WHERE open_time = $1")
    res <- dbxSelect(db, sql, params=params)
    expect_equal(res$count, 1)
  })
}
