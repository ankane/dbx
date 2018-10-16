runInsertTests <- function(db, redshift=FALSE) {
  test_that("insert works", {
    dbxDelete(db, "events")

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxSelect(db, "SELECT id, city FROM events ORDER BY id")
    expect_equal(res, events)
  })

  test_that("empty insert works", {
    dbxDelete(db, "events")

    dbxInsert(db, "events", data.frame())
    expect(TRUE)
  })

  test_that("insert returning works", {
    skip_if(!isPostgres(db) || redshift)

    dbxDelete(db, "events")

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    res <- dbxInsert(db, "events", events, returning=c("id", "city"))

    expect_equal(res$id, c(1, 2))
    expect_equal(res$city, events$city)
  })

  test_that("insert returning star works", {
    skip_if(!isPostgres(db) || redshift)

    dbxDelete(db, "events")

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    res <- dbxInsert(db, "events", events, returning=c("*"))

    expect_equal(res$id, c(1, 2))
    expect_equal(res$city, events$city)
  })

  test_that("insert batch size works", {
    dbxDelete(db, "events")

    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events, batch_size=1)

    res <- dbxSelect(db, "SELECT id, city FROM events")
    expect_equal(res, events)
  })
}
