parse_impala_query_output <- function(lines) {
  lines %>%
  { rawToChar(.$stdout) } %>%
    stringi::stri_split_lines() %>%
    purrr::flatten_chr() %>%
    # remove empty lines
    stringr::str_subset(pattern = "^$", negate = TRUE) %>%
    # remove delimiting lines
    stringr::str_subset(pattern = "^\\+-", negate = TRUE)
}
