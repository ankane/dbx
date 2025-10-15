runUpdateTests <- function(db) {
  test_that("update works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(2), city=c("LA"))
    dbxUpdate(db, "events", update_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "LA"))
  })

  test_that("fast update works", {
    skip_if(!fastUpdateSupported(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(2), city=c("LA"))
    dbxUpdate(db, "events", update_events, where_cols=c("id"), fast=TRUE)

    res <- dbxSelect(db, "SELECT city FROM events WHERE id = 2")
    expect_equal(res$city, c("LA"))
  })

  test_that("update multiple columns works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), counter=c(10, 11), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(1, 2), city=c("LA", "Boston"), counter=c(20, 21))
    dbxUpdate(db, "events", update_events, where_cols=c("id", "city"))

    res <- dbxSelect(db, "SELECT counter FROM events ORDER BY id")
    expect_equal(res$counter, c(10, 21))
  })

  test_that("fast update multiple columns works", {
    skip_if(!fastUpdateSupported(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), counter=c(10, 11), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(1, 2), city=c("LA", "Boston"), counter=c(20, 21))
    dbxUpdate(db, "events", update_events, where_cols=c("id", "city"), fast=TRUE)

    res <- dbxSelect(db, "SELECT counter FROM events")
    expect_equal(res$counter, c(10, 21))
  })

  test_that("update multiple columns where_cols order not important", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), counter=c(10, 11), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(1, 2), city=c("LA", "Boston"), counter=c(20, 21))
    dbxUpdate(db, "events", update_events, where_cols=c("city", "id"))

    res <- dbxSelect(db, "SELECT counter FROM events ORDER BY id")
    expect_equal(res$counter, c(10, 21))
  })

  test_that("fast update multiple columns where_cols order not important", {
    skip_if(!fastUpdateSupported(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), counter=c(10, 11), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(1, 2), city=c("LA", "Boston"), counter=c(20, 21))
    dbxUpdate(db, "events", update_events, where_cols=c("city", "id"), fast=TRUE)

    res <- dbxSelect(db, "SELECT counter FROM events")
    expect_equal(res$counter, c(10, 21))
  })

  test_that("update missing column raises error", {
    update_events <- data.frame(id=c(2), city=c("LA"))
    expect_error(dbxUpdate(db, "events", update_events, where_cols=c("missing")), "where_cols not in records")
  })

  test_that("empty update works", {
    dbxUpdate(db, "events", data.frame(id = as.numeric(), active = as.logical()), where_cols=c("id"))
    expect_true(TRUE)
  })

  test_that("update with transaction works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(2), city=c("LA"))
    DBI::dbWithTransaction(db, {
      dbxUpdate(db, "events", update_events, where_cols=c("id"), transaction=FALSE)
    })

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "LA"))
  })

  test_that("update schema DBI::SQL works", {
    skip_if(!isPostgres(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(2), city=c("LA"))
    dbxUpdate(db, DBI::SQL("public.events"), update_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "LA"))
  })

  test_that("update schema DBI:Id works", {
    skip_if(!isPostgres(db))

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(2), city=c("LA"))
    dbxUpdate(db, DBI::Id(schema="public", table="events"), update_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "LA"))
  })
}
