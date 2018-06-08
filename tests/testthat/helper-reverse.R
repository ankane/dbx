reverse <- function(df) {
  df <- df[seq(dim(df)[1], 1), ]
  rownames(df) <- NULL
  df
}
