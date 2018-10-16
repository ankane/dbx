# set time zone
Sys.setenv(TZ="America/Los_Angeles")

runTests <- function(db, redshift=FALSE) {
  assign(".testthat_db", db, envir = .GlobalEnv)

  runSelectTests(db)
  runInsertTests(db, redshift=redshift)
  runUpdateTests(db)
  runUpsertTests(db, redshift=redshift)
  runDeleteTests(db)

  rm(".testthat_db", envir = .GlobalEnv)

  dbxDisconnect(db)
}
