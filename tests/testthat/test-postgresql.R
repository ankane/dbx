context("postgresql")

skip_on_cran()

# https://github.com/tomoakin/RPostgreSQL/pull/119
# TODO remove when released
skip_on_os("windows")

db <- dbxConnect(adapter="rpostgresql", dbname="dbx_test")

dbxExecute(db, "DROP TABLE IF EXISTS events")
dbxExecute(db, "CREATE TABLE events (id SERIAL PRIMARY KEY, city TEXT, counter INT, bigcounter BIGINT, speed FLOAT, distance DECIMAL, created_on DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ, open_time TIME, close_time TIMETZ, active BOOLEAN, properties JSON, propertiesb JSONB, image BYTEA)")

runTests(db)

test_that("datetimes with storage_tz works", {
  inTimeZone("America/Chicago", {
    db2 <- dbxConnect(adapter="rpostgresql", dbname="dbx_test", storage_tz="America/Chicago")
    dbxDelete(db2, "events")

    t1 <- as.POSIXct("2018-01-01 12:30:55", tz="America/New_York")
    t2 <- as.POSIXct("2018-01-01 16:59:59", tz="America/New_York")
    events <- data.frame(updated_at=c(t1, t2))
    dbxInsert(db2, "events", events)

    # for R-devel
    attr(events$updated_at, "tzone") <- Sys.timezone()

    # test returned time
    res <- dbxSelect(db2, "SELECT * FROM events ORDER BY id")
    expect_equal(res$updated_at, events$updated_at)

    # test stored time
    res <- dbxSelect(db2, "SELECT COUNT(*) AS count FROM events WHERE updated_at = '2018-01-01 11:30:55'")
    expect_equal(1, res$count)

    dbxDisconnect(db2)
  })
})
