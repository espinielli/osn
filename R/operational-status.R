#' Get operational status for a time period
#'
#' @param session      SSH session to OSN Impala
#' @param icao24       (Optional) Single or vector of ICAO24 ICAO 24-bit addresses
#'                     among those in the bounding box
#' @param wef_time     Start of period of interest
#' @param til_time     (Optional) End of period of interest, if NULL wef_time + 1 day
#' @param bbox         (Optional) axis aligned bounding box
#'                     (lon_min, lat_min, lon_max, lat_max)
#' @param debug_level  one of "DEBUG", "TRACE" or NULL (default)
#'                     It prints usefult debug info, i.e. input args, query string,
#'                     or traces the query results in the file `query_output.txt`.
#'
#' @return  a dataframe of operational status info (icao24, hour, rawmsg)
#' @export
#'
#' @examples
#' \dontrun{
#' osn:::operational_status(
#'   session = 1,
#'   icao24 = NULL,
#'   wef_time = "2019-01-01 00:00:00",
#'   til_time = "2019-01-01 02:00:00",
#'   bbox = c(xmin = 7.536746, xmax = 9.604390, ymin = 49.36732, ymax = 50.69920),
#'   debug_level = "DEBUG"
#' )
#' }

operational_status <- function(
  session,
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
    "SELECT DISTINCT {COLUMNS} FROM operational_status_data4 os {OTHER_TABLES} ",
    "WHERE os.hour >= {WEFH} and os.hour < {TILH} ",
    "and os.hour = sv.hour ",
    "and os.icao24 = sv.icao24 ",
    "{OTHER_PARAMS} ",
    "{ORDER};",
    # COLUMNS = "os.icao24, os.hour, mintime, maxtime, os.msgcount, os.rawmsg",
    COLUMNS = "os.icao24, os.hour, os.rawmsg",
    WEFH = wef_time,
    TILH = til_time,
    ICAO24 = icao24,
    OTHER_TABLES = ", state_vectors_data4 sv",
    OTHER_PARAMS = other_params,
    ORDER = "ORDER BY os.hour, os.icao24")
  logger::log_debug('Impala query = {query}')
  # select
  #   icao24, hour, mintime, maxtime, msgcount, rawmsg
  # from
  #   operational_status_data4
  # where
  #   hour = 1546300800
  #   and unique(icao24) in (
  #          select
  #             icao24
  #          from
  #             state_vectors_data4
  #          where
  #             hour = 1546300800
  #             and ((7.553013 <= lon and lon <= 9.585482) and (49.378819 <= lat and lat <= 50.688044))
  #   );

}

# [hadoop-1:21000] > describe operational_status_data4;
# +---------------------------------+-------------------+---------+
#   | name                            | type              | comment |
#   +---------------------------------+-------------------+---------+
#   | sensors                         | array<struct<     |         |
#   |                                 |   serial:int,     |         |
#   |                                 |   mintime:double, |         |
#   |                                 |   maxtime:double  |         |
#   |                                 | >>                |         |
#   | rawmsg                          | string            |         |
#   | icao24                          | string            |         |
#   | mintime                         | double            |         |
#   | maxtime                         | double            |         |
#   | msgcount                        | bigint            |         |
#   | subtypecode                     | tinyint           |         |
#   | unknowncapcode                  | boolean           |         |
#   | unknownopcode                   | boolean           |         |
#   | hasoperationaltcas              | smallint          |         |
#   | has1090esin                     | boolean           |         |
#   | supportsairreferencedvelocity   | smallint          |         |
#   | haslowtxpower                   | smallint          |         |
#   | supportstargetstatereport       | smallint          |         |
#   | supportstargetchangereport      | smallint          |         |
#   | hasuatin                        | boolean           |         |
#   | nacv                            | tinyint           |         |
#   | nicsupplementc                  | smallint          |         |
#   | hastcasresolutionadvisory       | boolean           |         |
#   | hasactiveidentswitch            | boolean           |         |
#   | usessingleantenna               | boolean           |         |
#   | systemdesignassurance           | tinyint           |         |
#   | gpsantennaoffset                | tinyint           |         |
#   | airplanelength                  | int               |         |
#   | airplanewidth                   | double            |         |
#   | version                         | tinyint           |         |
#   | nicsupplementa                  | boolean           |         |
#   | positionnac                     | double            |         |
#   | geometricverticalaccuracy       | int               |         |
#   | sourceintegritylevel            | tinyint           |         |
#   | barometricaltitudeintegritycode | smallint          |         |
#   | trackheadinginfo                | smallint          |         |
#   | horizontalreferencedirection    | boolean           |         |
#   | hour                            | int               |         |
#   +---------------------------------+-------------------+---------+

  cols <- readr::cols(
    .default      = readr::col_character(),
    # mintime       = readr::col_double(),
    # maxtime       = readr::col_double(),
    icao24        = readr::col_character(),
    rawmsg        = readr::col_character(),
    # msgcount      = readr::col_double(),
    hour          = readr::col_integer()
  )
  # impala_query(session, query, cols)


