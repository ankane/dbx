test_that <- function(desc, code) {
  if (exists(".testthat_db", envir = .GlobalEnv)) {
    db <- get(".testthat_db", envir = .GlobalEnv)
    dbxDelete(db, "events")
  }
  testthat::test_that(desc, code)
}
