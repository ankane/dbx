inTimeZone <- function(tz, code) {
  previous_tz <- currentTimeZone()
  tryCatch({
    Sys.setenv(TZ=tz)
    eval(code)
  }, finally={
    Sys.setenv(TZ=previous_tz)
  })
}

currentTimeZone <- function() {
  Sys.getenv("TZ", Sys.timezone())
}
