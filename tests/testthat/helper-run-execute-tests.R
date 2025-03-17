runExecuteTests <- function(db) {
  test_that("execute works", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxExecute(db, "UPDATE events SET city = 'New York' WHERE id = 1")
    expect_equal(res, 1)
  })

  test_that("params work", {
    events <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
    dbxInsert(db, "events", events)

    res <- dbxExecute(db, "UPDATE events SET city = ? WHERE id IN (?)", params=list("New York", 1:2))
    expect_equal(res, 2)
  })
}
