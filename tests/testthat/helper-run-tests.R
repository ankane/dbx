# set time zone
Sys.setenv(TZ="America/Los_Angeles")

runTests <- function(db, redshift=FALSE) {
  runSelectTests(db)
  runInsertTests(db, redshift=redshift)
  runUpdateTests(db)
  runUpsertTests(db, redshift=redshift)
  runDeleteTests(db)

  dbxDisconnect(db)
}
