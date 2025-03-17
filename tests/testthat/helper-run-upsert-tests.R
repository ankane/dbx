runUpsertTests <- function(db, redshift=FALSE) {
  upsertSupported <- function() {
    !redshift
  }

  test_that("upsert works", {
    skip_if_not(upsertSupported())

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), counter=c(4, 5), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    upsert_events <- data.frame(id=c(2, 3), city=c("Chicago", "New York"))
    dbxUpsert(db, "events", upsert_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT id, city, counter FROM events ORDER BY id")
    expect_equal(res$id, c(1, 2, 3))
    expect_equal(res$city, c("San Francisco", "Chicago", "New York"))
    expect_equal(res$counter, c(4, 5, NA))
  })

  test_that("upsert only where_cols works", {
    skip_if_not(upsertSupported())

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    upsert_events <- data.frame(id=c(2, 3))
    dbxUpsert(db, "events", upsert_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "Boston", NA))
  })

  test_that("upsert skip_existing works", {
    skip_if_not(upsertSupported())

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    upsert_events <- data.frame(id=c(2, 3), city=c("Chicago", "New York"))
    dbxUpsert(db, "events", upsert_events, where_cols=c("id"), skip_existing=TRUE)

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "Boston", "New York"))
  })

  # TODO test upsert multiple columns works
  # TODO test upsert multiple columns where_cols order not important

  test_that("upsert missing column raises error", {
    skip_if_not(upsertSupported())

    update_events <- data.frame(id=c(3), city=c("LA"))
    expect_error(dbxUpsert(db, "events", update_events, where_cols=c("missing")), "where_cols not in records")
  })

  test_that("empty upsert works", {
    dbxUpsert(db, "events", data.frame(id = as.numeric(), active = as.logical()), where_cols=c("id"))
    expect_true(TRUE)
  })

  test_that("upsert returning works", {
    skip_if(!returningSupported(db) || redshift)

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    res <- dbxUpsert(db, "events", events, where_cols=c("id"), returning=c("id", "city"))

    expect_equal(res$id, c(1, 2))
    expect_equal(res$city, events$city)

    upsert_events <- data.frame(id=c(2, 3), city=c("Chicago", "New York"))
    res <- dbxUpsert(db, "events", upsert_events, where_cols=c("id"), returning=c("id", "city"))

    expect_equal(res$id, c(2, 3))
    expect_equal(res$city, upsert_events$city)
  })

  test_that("upsert skip_existing returning works", {
    skip_if(!returningSupported(db) || redshift)

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    res <- dbxUpsert(db, "events", events, where_cols=c("id"), skip_existing=TRUE, returning=c("id", "city"))

    expect_equal(res$id, c(1, 2))
    expect_equal(res$city, events$city)

    upsert_events <- data.frame(id=c(2, 3), city=c("Chicago", "New York"))
    res <- dbxUpsert(db, "events", upsert_events, where_cols=c("id"), skip_existing=TRUE, returning=c("id", "city"))

    if (isMariaDB(db)) {
      expect_equal(res$id, c(2, 3))
      expect_equal(res$city, c("Boston", "New York"))
    } else {
      expect_equal(res$id, c(3))
      expect_equal(res$city, c("New York"))
    }
  })

  test_that("upsert returning inserted works", {
    skip_if(!isPostgres(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    upsert_events <- data.frame(id=c(2, 3), city=c("Chicago", "New York"))
    res <- dbxUpsert(db, "events", upsert_events, where_cols=c("id"), returning=DBI::SQL("(xmax = 0) AS inserted"))

    if (isODBCPostgres(db)) {
      res$inserted <- res$inserted != "0"
    }
    expect_equal(res$inserted, c(FALSE, TRUE))
  })

  test_that("upsert schema DBI::SQL works", {
    skip_if(!isPostgres(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    upsert_events <- data.frame(id=c(2, 3), city=c("Chicago", "New York"))
    dbxUpsert(db, DBI::SQL("public.events"), upsert_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "Chicago", "New York"))
  })

  test_that("upsert schema DBI::Id works", {
    skip_if(!isPostgres(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    upsert_events <- data.frame(id=c(2, 3), city=c("Chicago", "New York"))
    dbxUpsert(db, DBI::Id(schema="public", table="events"), upsert_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "Chicago", "New York"))
  })

  test_that("upsert NA works", {
    skip_if_not(upsertSupported())

    # https://github.com/ankane/dbx/issues/30
    # https://github.com/r-dbi/RPostgres/issues/393
    skip_if(isRPostgres(db))

    events <- data.frame(id=c(1, 2), created_on=c("2022-01-01", NA), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)
  })
}
