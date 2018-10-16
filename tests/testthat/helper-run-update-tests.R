runUpdateTests <- function(db, redshift=FALSE) {
  test_that("update works", {
    dbxDelete(db, "events")

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    update_events <- data.frame(id=c(2), city=c("LA"))
    dbxUpdate(db, "events", update_events, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM events WHERE id = 2")
    expect_equal(res$city, c("LA"))
  })

  test_that("update missing column raises error", {
    dbxDelete(db, "events")

    update_events <- data.frame(id=c(2), city=c("LA"))
    expect_error(dbxUpdate(db, "events", update_events, where_cols=c("missing")), "where_cols not in records")
  })

  test_that("empty update works", {
    dbxDelete(db, "events")

    dbxUpdate(db, "events", data.frame(id = as.numeric(), active = as.logical()), where_cols=c("id"))
    expect(TRUE)
  })
}
