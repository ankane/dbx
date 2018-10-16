runDeleteTests <- function(db) {
  test_that("delete all works", {
    dbxDelete(db, "events")

    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events")
    expect_equal(res$count, 0)
  })

  test_that("delete empty does not delete rows", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    delete_events <- data.frame(id=c())
    dbxDelete(db, "events", where=delete_events)

    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM events")
    expect_equal(res$count, 2)
  })

  test_that("delete one column works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    delete_events <- data.frame(id=c(2))
    dbxDelete(db, "events", where=delete_events)

    res <- dbxSelect(db, "SELECT id, city FROM events ORDER BY id ASC")
    expect_equal(res, events[1, ])
  })

  test_that("delete multiple columns works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    dbxDelete(db, "events", where=events[2, ])

    res <- dbxSelect(db, "SELECT id, city FROM events ORDER BY id ASC")
    expect_equal(res, events[1, ])
  })
}
