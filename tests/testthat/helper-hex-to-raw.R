hexToRaw <- function(x) {
  y <- strsplit(x, "")[[1]]
  z <- paste0(y[c(TRUE, FALSE)], y[c(FALSE, TRUE)])
  as.raw(as.hexmode(z))
}
