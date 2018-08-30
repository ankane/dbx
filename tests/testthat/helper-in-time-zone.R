inTimeZone <- function(tz, code) {
  previous_tz <- Sys.getenv("TZ", Sys.timezone())
  tryCatch({
    Sys.setenv(TZ=tz)
    eval(code)
  }, finally={
    Sys.setenv(TZ=previous_tz)
  })
}

# set time zone
Sys.setenv(TZ="America/Los_Angeles")
