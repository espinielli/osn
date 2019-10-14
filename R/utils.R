parse_impala_query_output <- function(lines) {
  lines %>%
    stringi::stri_split_lines() %>%
    purrr::flatten_chr() %>%
    # remove empty lines
    stringr::str_subset(pattern = "^$", negate = TRUE) %>%
    # remove delimiting lines
    stringr::str_subset(pattern = "^\\+-", negate = TRUE) %>%
    # remove blanks
    stringr::str_replace_all(pattern = "[ ][ ]*", "") %>%
    # remove leading/last '|'
    stringr::str_replace(pattern = "^\\|", "") %>%
    stringr::str_replace(pattern = "\\|$", "") %>%
    unique()
}
