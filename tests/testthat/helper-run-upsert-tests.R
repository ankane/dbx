runUpsertTests <- function(db, redshift=FALSE) {
  test_that("upsert works", {
    skip_if(isSQLite(db) || redshift || isSQLServer(db))

    upsert_orders <- data.frame(id=c(3, 5), city=c("Boston", "Chicago"))
    dbxUpsert(db, "orders", upsert_orders, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM orders WHERE id IN (3, 5)")
    expect_equal(res$city, c("Boston", "Chicago"))

    dbxDelete(db, "orders", data.frame(id=c(5)))
  })

  test_that("upsert only where_cols works", {
    skip_if(isSQLite(db) || redshift || isSQLServer(db))

    upsert_orders <- data.frame(id=c(3, 5))
    dbxUpsert(db, "orders", upsert_orders, where_cols=c("id"))

    res <- dbxSelect(db, "SELECT city FROM orders WHERE id IN (3, 5)")
    expect_equal(res$city, c("Boston", NA))

    dbxDelete(db, "orders", data.frame(id=c(5)))
  })

  test_that("upsert missing column raises error", {
    update_orders <- data.frame(id=c(3), city=c("LA"))
    expect_error(dbxUpsert(db, "orders", update_orders, where_cols=c("missing")), "where_cols not in records")
  })

  test_that("empty upsert works", {
    skip_if(!isPostgres(db) || redshift)

    dbxUpsert(db, "events", data.frame(id = as.numeric(), active = as.logical()), where_cols=c("id"))
    expect(TRUE)
  })
}
