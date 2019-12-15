#' @importFrom magrittr %>%
#' @export
magrittr::`%>%`

#' @importFrom rlang .data
#' @export
rlang::`.data`

#' @importFrom askpass askpass
#' @export
askpass::askpass

# take care of R CHECK's NOTE about "no visible binding for global variable '.'"
# see https://github.com/tidyverse/magrittr/issues/29
if(getRversion() >= "2.15.1")  utils::globalVariables(c("."))


.onLoad <- function(libname, pkgname) {
  # --- from botor pkg ---
  ## although glue would be more convenient,
  ## but let's use the always available sprintf formatter function for logging
  # logger::log_formatter(logger::formatter_sprintf, namespace = pkgname)
  logger::log_threshold(logger::INFO, namespace = pkgname)
}
