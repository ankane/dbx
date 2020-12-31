runInsertTests <- function(db, redshift=FALSE) {
  test_that("insert works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT id, city FROM events ORDER BY id")
    expect_equal(res, events)
  })

  test_that("empty insert works", {
    dbxInsert(db, "events", data.frame())
    expect_true(TRUE)
  })

  test_that("insert returning works", {
    skip_if(!isPostgres(db) || redshift)

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    res <- dbxInsert(db, "events", events, returning=c("id", "city"))

    expect_equal(res$id, c(1, 2))
    expect_equal(res$city, events$city)
  })

  test_that("insert returning star works", {
    skip_if(!isPostgres(db) || redshift)

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    res <- dbxInsert(db, "events", events, returning=c("*"))

    expect_equal(res$id, c(1, 2))
    expect_equal(res$city, events$city)
  })

  test_that("insert batch size works", {
    events <- data.frame(id=c(1, 2, 3), city=c("San Francisco", "Boston", "Chicago"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events, batch_size=2)

    res <- dbxSelect(db, "SELECT id, city FROM events")
    expect_equal(res, events)
  })

  test_that("insert factors work", {
    events <- data.frame(city=c("San Francisco", "Boston"))
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT * FROM events")
    expect_equal(res$city, as.character(events$city))
  })

  test_that("insert schema DBI::SQL works", {
    skip_if(!isPostgres(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, DBI::SQL("public.events"), events)

    res <- dbxSelect(db, "SELECT id, city FROM events ORDER BY id")
    expect_equal(res, events)
  })

  test_that("insert schema DBI::Id works", {
    skip_if(!isPostgres(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, DBI::Id(schema="public", table="events"), events)

    res <- dbxSelect(db, "SELECT id, city FROM events ORDER BY id")
    expect_equal(res, events)
  })
}
