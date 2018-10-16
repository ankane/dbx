# set time zone
Sys.setenv(TZ="America/Los_Angeles")

runTests <- function(db, redshift=FALSE) {
  orders <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
  new_orders <- data.frame(id=c(3, 4), city=c("New York", "Atlanta"), stringsAsFactors=FALSE)

  dbxInsert(db, "orders", orders)

  test_that("select works", {
    res <- dbxSelect(db, "SELECT id, city FROM orders ORDER BY id ASC")
    expect_equal(res, orders)
  })

  test_that("select order works", {
    res <- dbxSelect(db, "SELECT id, city FROM orders ORDER BY id DESC")
    expect_equal(res, reverse(orders))
  })

  test_that("select columns works", {
    res <- dbxSelect(db, "SELECT id FROM orders ORDER BY id ASC")
    expect_equal(res, orders[c("id")])
  })

  test_that("empty select works", {
    dbxDelete(db, "events")
    res <- dbxSelect(db, "SELECT * FROM events")
    expect_equal(nrow(res), 0)
  })

  test_that("empty result", {
    dbxDelete(db, "events")

    res <- dbxSelect(db, "SELECT * FROM events")

    # numeric
    expect_identical(res$counter, as.integer())
    expect_identical(res$speed, as.numeric())
    expect_identical(res$distance, as.numeric())

    # dates and times
    if (isSQLite(db)) {
      # empty datetimes are numeric
      expect_identical(res$created_on, as.numeric())
      expect_identical(res$updated_at, as.numeric())
    } else {
      expect_identical(res$created_on, as.Date(as.character()))

      # not identical due to empty tzone attribute
      expect_equal(res$updated_at, as.POSIXct(as.character()))
      expect_equal(res$deleted_at, as.POSIXct(as.character()))

      expect_identical(res$open_time, as.character())
    }

    # json
    expect_identical(res$properties, as.character())

    # booleans
    if (isRMariaDB(db)) {
      # until proper typecasting
      expect_identical(res$active, as.integer())
    } else if (isSQLite(db)) {
      # until proper typecasting
      expect_identical(res$active, as.numeric())
    } else if (isODBCPostgres(db)) {
      expect_identical(res$active, as.character())
    } else {
      expect_identical(res$active, as.logical())
    }

    # binary
    if (isRMySQL(db)) {
      # no way to tell text and blobs apart
      expect_identical(class(res$image), "character")
    } else {
      expect_identical(class(res$image), "list")
    }
  })

  test_that("empty cells", {
    dbxDelete(db, "events")

    dbxInsert(db, "events", data.frame(properties=NA))
    res <- dbxSelect(db, "SELECT * FROM events")

    # numeric
    expect_identical(res$counter, as.integer(NA))
    expect_identical(res$speed, as.numeric(NA))
    expect_identical(res$distance, as.numeric(NA))

    # dates and times
    if (isSQLite(db)) {
      # empty datetimes are numeric
      expect_identical(res$created_on, as.numeric(NA))
      expect_identical(res$updated_at, as.numeric(NA))
    } else {
      expect_identical(res$created_on, as.Date(NA))

      # not identical due to empty tzone attribute
      expect_equal(res$updated_at, as.POSIXct(NA))
      expect_equal(res$deleted_at, as.POSIXct(NA))

      expect_identical(res$open_time, as.character(NA))
    }

    # json
    expect_identical(res$properties, as.character(NA))

    # booleans
    if (isRMariaDB(db)) {
      # until proper typecasting
      expect_identical(res$active, as.integer(NA))
    } else if (isSQLite(db)) {
      # until proper typecasting
      expect_identical(res$active, as.numeric(NA))
    } else if (isODBCPostgres(db)) {
      expect_identical(res$active, as.character(NA))
    } else {
      expect_identical(res$active, NA)
    }

    # binary
    if (isRMySQL(db)) {
      # no way to tell text and blobs apart
      expect_identical(class(res$image), "character")
    } else {
      expect_identical(class(res$image), "list")

      if (isRMariaDB(db)) {
        expect_identical(class(res$image[[1]]), "NULL")
      } else {
        expect_identical(res$image[[1]], as.raw(NULL))
      }
    }
  })

  test_that("insert works", {
    dbxInsert(db, "orders", new_orders)

    res <- dbxSelect(db, "SELECT * FROM orders WHERE id > 2 ORDER BY id")
    expect_equal(res$city, new_orders$city)
  })

  test_that("update works", {
    update_orders <- data.frame(id=c(3), city=c("LA"))
    dbxUpdate(db, "orders", update_orders, where_cols=c("id"))
    res <- dbxSelect(db, "SELECT city FROM orders WHERE id = 3")
    expect_equal(res$city, c("LA"))
  })

  test_that("update missing column raises error", {
    update_orders <- data.frame(id=c(3), city=c("LA"))
    expect_error(dbxUpdate(db, "orders", update_orders, where_cols=c("missing")), "where_cols not in records")
  })

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

  test_that("delete empty does not delete rows", {
    delete_orders <- data.frame(id=c())
    dbxDelete(db, "orders", where=delete_orders)
    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM orders")
    expect_equal(res$count, 4)
  })

  test_that("delete one column works", {
    delete_orders <- data.frame(id=c(3))
    dbxDelete(db, "orders", where=delete_orders)
    res <- dbxSelect(db, "SELECT id, city FROM orders ORDER BY id ASC")
    exp <- rbind(orders, new_orders)[c(1, 2, 4), ]
    rownames(exp) <- NULL
    expect_equal(res, exp)
  })

  test_that("delete multiple columns works", {
    dbxDelete(db, "orders", where=orders)
    res <- dbxSelect(db, "SELECT id, city FROM orders ORDER BY id ASC")
    exp <- new_orders[c(2), ]
    rownames(exp) <- NULL
    expect_equal(res, exp)
  })

  test_that("delete all works", {
    dbxDelete(db, "orders")
    res <- dbxSelect(db, "SELECT COUNT(*) AS count FROM orders")
    expect_equal(res$count, 0)
  })

  test_that("empty insert works", {
    dbxInsert(db, "events", data.frame())
    expect(TRUE)
  })

  test_that("empty update works", {
    dbxUpdate(db, "events", data.frame(id = as.numeric(), active = as.logical()), where_cols=c("id"))
    expect(TRUE)
  })

  test_that("empty upsert works", {
    skip_if(!isPostgres(db) || redshift)

    dbxUpsert(db, "events", data.frame(id = as.numeric(), active = as.logical()), where_cols=c("id"))
    expect(TRUE)
  })

  test_that("insert returning works", {
    skip_if(!isPostgres(db) || redshift)

    res <- dbxInsert(db, "orders", orders[c("city")], returning=c("id", "city"))
    expect_equal(res$id, c(1, 2))
    expect_equal(res$city, orders$city)

    res <- dbxInsert(db, "orders", orders[c("city")], returning="*")
    expect_equal(res$id, c(3, 4))
    expect_equal(res$city, orders$city)
  })

  test_that("insert batch size works", {
    dbxDelete(db, "orders")
    dbxInsert(db, "orders", orders, batch_size=1)

    res <- dbxSelect(db, "SELECT id, city FROM orders")
    expect_equal(res, orders)
  })

  runDataTypeTests(db, redshift=redshift)

  if (!isRMySQL(db)) {
    runParamsTests(db)
  }

  dbxDisconnect(db)
}
