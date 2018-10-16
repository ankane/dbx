# set time zone
Sys.setenv(TZ="America/Los_Angeles")

runTests <- function(db, redshift=FALSE) {
  # orders <- data.frame(id=c(1, 2), city=c("San Francisco", "Boston"), stringsAsFactors=FALSE)
  # new_orders <- data.frame(id=c(3, 4), city=c("New York", "Atlanta"), stringsAsFactors=FALSE)

  # dbxInsert(db, "orders", orders)

  runSelectTests(db)
  runInsertTests(db, redshift=redshift)
  runUpdateTests(db)
  runUpsertTests(db, redshift=redshift)
  runDeleteTests(db)

  dbxDisconnect(db)
}
