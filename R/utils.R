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
    stringr::str_replace_all("^[|](.+)[|]$", "\\1") %>%
    # remove duplicated lines, i.e. repeated column names header
    unique()
}


impala_query <- function(session, query, cols) {
  stopifnot(!is.null(cols))
  lines <- ssh::ssh_exec_internal(
    session,
    stringr::str_glue("-q {query}", query = query)) %>%
    { rawToChar(.$stdout) }
  if (logger::log_threshold() == logger::TRACE) {
    lines %>%
      readr::write_lines("query_output.txt")
  }
  lines <- lines %>%
    parse_impala_query_output()

  # make a 1 line data so to return an empty tibble in case of empty Impala result
  if (length(lines) == 0) lines <- paste0(paste(names(cols$cols), collapse = "|"),
                                          "\n")

  lines %>%
    readr::read_delim(col_types = cols,
                      delim = "|",
                      na = c("", "NULL"),
                      trim_ws = TRUE) %>%
    janitor::clean_names()
}
