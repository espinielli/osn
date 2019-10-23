#' Get state vectors for a time period
#'
#' @param session  SSH session to OSN Impala
#' @param icao24   (Optional) Single or vector of ICAO24 ICAO 24-bit addresses
#' @param wef_time Start of period of interest
#' @param til_time (Optional) End of period of interest, if NULL wef_time + 1 day
#' @param bbox     (Optional) axis aligned bounding box
#'                 (lon_min, lat_min, lon_max, lat_max)
#' @param debug_level one of "DEBUG", "TRACE" or NULL (default)
#'                 It prints usefult debug info, i.e. input args, query string,
#'                 or traces the query results in the file `query_output.txt`.
#'
#' @return a dataframe of state vectors
#' @export
#'
#' @examples
#' \dontrun{
#' session <- connect_osn("cucu", verbose = 2)
#' state_vector(
#'    session,
#'    icao24 = c("3c6589", "3c6757"),
#'    wef_time = "2019-04-22 00:00:00",
#'    til_time = "2019-04-22 10:00:00"
#' )
#' }
state_vector <- function(session,
                         icao24,
                         wef_time, til_time = NULL,
                         bbox = NULL,
                         debug_level = NULL
                         ) {

  if (!is.null(debug_level)) {
    switch (debug_level,
            "INFO" = {logger::log_threshold(logger::INFO)},
            "DEBUG" = {logger::log_threshold(logger::DEBUG)},
            "TRACE" = {logger::log_threshold(logger::TRACE)}
    )
  }

  wef_time <- lubridate::ymd_hms(wef_time)
  logger::log_debug('Input argument wef_time = {format(wef_time, "%Y-%m-%d %H:%M:%S")}')
  if (is.null(til_time)) {
    til_time <- wef_time + lubridate::days(1)
  } else {
    til_time <- lubridate::ymd_hms(til_time)
  }
  logger::log_debug('Input argument til_time = {format(til_time, "%Y-%m-%d %H:%M:%S")}')
  wef_time <- as.numeric(wef_time)
  til_time <- as.numeric(til_time)
  other_params <- " "

  if (!is.null(icao24)) {
    icao24 <- paste0("'", icao24, "'") %>%
      stringr::str_c(collapse = ",")
    other_params <- stringr::str_glue(
      other_params,
      " AND icao24 in ({ICAO24}) ",
      ICAO24 = icao24)
  }
  if (!is.null(bbox)) {
    other_params <- stringr::str_glue(
      other_params,
      " AND (({lon_min} <= lon AND lon <={lon_max}) AND ({lat_min} <= lat AND lat <={lat_max}))",
      lon_min = bbox[1],
      lon_max = bbox[3],
      lat_min = bbox[2],
      lat_max = bbox[4])
  }

  query <- stringr::str_glue(
    "SELECT {COLUMNS} FROM state_vectors_data4 {OTHER_TABLES} ",
    "WHERE hour >= {WEFH} and hour < {TILH} ",
    # "and time >= {WEFT} and time < {WEFT} ",
    "{OTHER_PARAMS};",
    COLUMNS = "*",
    WEFH = wef_time,
    TILH = til_time,
    ICAO24 = icao24,
    OTHER_TABLES = "",
    OTHER_PARAMS = other_params)
  logger::log_debug('Impala query = {query}')
  #   | time          | int        |
  #   | icao24        | string     |
  #   | lat           | double     |
  #   | lon           | double     |
  #   | velocity      | double     |
  #   | heading       | double     |
  #   | vertrate      | double     |
  #   | callsign      | string     |
  #   | onground      | boolean    |
  #   | alert         | boolean    |
  #   | spi           | boolean    |
  #   | squawk        | string     |
  #   | baroaltitude  | double     |
  #   | geoaltitude   | double     |
  #   | lastposupdate | double     |
  #   | lastcontact   | double     |
  #   | serials       | array<int> |
  #   | hour          | int        |                             |

  cols <- readr::cols(
    .default      = readr::col_double(),
    time          = readr::col_integer(),
    icao24        = readr::col_character(),
    lat           = readr::col_double(),
    lon           = readr::col_double(),
    velocity      = readr::col_double(),
    heading       = readr::col_double(),
    vertrate      = readr::col_double(),
    callsign      = readr::col_character(),
    onground      = readr::col_logical(),
    alert         = readr::col_logical(),
    spi           = readr::col_logical(),
    squawk        = readr::col_character(),
    baroaltitude  = readr::col_double(),
    geoaltitude   = readr::col_double(),
    lastposupdate = readr::col_double(),
    lastcontact   = readr::col_double(),
    hour          = readr::col_integer()
  )
  impala_query(session, query, cols)
}
