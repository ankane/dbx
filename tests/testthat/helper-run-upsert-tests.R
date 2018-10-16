runUpsertTests <- function(db, redshift=FALSE) {
  test_that("upsert works", {
    skip_if(isSQLite(db) || redshift || isSQLServer(db))

    dbxDelete(db, "events")

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    upsert_events <- data.frame(id=c(2, 3), city=c("Chicago", "New York"))
    dbxUpsert(db, "events", upsert_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "Chicago", "New York"))
  })

  test_that("upsert only where_cols works", {
    skip_if(isSQLite(db) || redshift || isSQLServer(db))

    dbxDelete(db, "events")

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    upsert_events <- data.frame(id=c(2, 3))
    dbxUpsert(db, "events", upsert_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM events ORDER BY id")
    expect_equal(res$city, c("San Francisco", "Boston", NA))
  })

  test_that("upsert missing column raises error", {
    update_events <- data.frame(id=c(3), city=c("LA"))
    expect_error(dbxUpsert(db, "events", update_events, where_cols=c("missing")), "where_cols not in records")
  })

  test_that("empty upsert works", {
    skip_if(!isPostgres(db) || redshift)

    dbxDelete(db, "events")

    dbxUpsert(db, "events", data.frame(id = as.numeric(), active = as.logical()), where_cols=c("id"))
    expect(TRUE)
  })
}
