#' Get state vectors for a time period
#'
#' @param session  SSH session to OSN Impala
#' @param icao24   (Optional) Single or vector of ICAO24 ICAO 24-bit addresses
#' @param wef      Start of period of interest (date or datetime)
#' @param til      (Optional) End of period of interest, if NULL wef_time + 1 day
#' @param bbox     (Optional) axis aligned bounding box like
#'                 `c(xmin, xmax, ymin, ymax)`
#' @return data frame of state vector data containing the following
#'         variables (see also OSN docs about
#'   \href{https://opensky-network.org/apidoc/rest.html#arrivals-by-airport}{State Vector}):
#'    \tabular{lll}{
#'      \strong{Name}       \tab \strong{Description} \tab \strong{Type} \cr
#'      icao24              \tab ICAO 24-bit address \tab chr \cr
#'      callsign            \tab flight's callsign   \tab chr \cr
#'      estdepartureairport \tab Estimated departure airport \tab chr \cr
#'      estarrivalairport   \tab Estimated arrival airport   \tab chr \cr
#'      start               \tab Start of portion of trajectory: `min(firstseen, lastseen - duration)` \tab int \cr
#'      firstseen           \tab first seen by OpenSky Network (UNIX timestamp)\tab int \cr
#'      lastseen            \tab last seen by OpenSky Network (UNIX timestamp) \tab int \cr
#'      item.time           \tab position report's time (UNIX timestamp) \tab int \cr
#'      item.longitude      \tab position report's longitude (WSG84 decimal degrees)\tab dbl \cr
#'      item.latitude       \tab position report's latitude (WSG84 decimal degrees) \tab dbl \cr
#'      item.velocity       \tab ground speed of the aircraft (m/s) \tab dbl \cr
#'      item.heading        \tab true track in decimal degrees clockwise from north (north=0°) \tab dbl \cr
#'      item.vertrate       \tab vertical speed of the aircraft (m/s) \tab dbl \cr
#'      item.onground       \tab TRUE if the position was retrieved from a surface position report \tab lgl \cr
#'      item.baroaltitude   \tab position report's barometric altitude (m) \tab dbl \cr
#'      item.geoaltitude    \tab position report's GNSS (GPS) altitude (m) \tab dbl \cr
#'      item.hour           \tab position report's hour (UNIX timestamp) \tab int
#'    }
#' @export
#'
#' @examples
#' \dontrun{
#' session <- connect_osn("cucu", verbose = 2)
#' state_vector(
#'    session,
#'    icao24 = c("3c6589", "3c6757"),
#'    wef = "2019-04-22 00:00:00",
#'    til = "2019-04-22 10:00:00",
#'    bbox = c(xmin = 7.536746, xmax = 9.604390, ymin = 49.36732, ymax = 50.69920)
#' )
#' }
state_vector <- function(session,
                         icao24,
                         wef, til = NULL,
                         bbox = NULL
                         ) {
  wef <- lubridate::as_datetime(wef)
  stopifnot(class(wef) %in% c("POSIXct", "POSIXt"))

  if (is.null(til)) {
    til <- wef + lubridate::days(1)
  } else {
    til <- lubridate::as_datetime(til)
  }
  logger::log_trace('Input argument wef = {format(wef, "%Y-%m-%d %H:%M:%S")}')
  logger::log_trace('Input argument til = {format(til, "%Y-%m-%d %H:%M:%S")}')

  wef <- as.numeric(wef)
  til <- as.numeric(til)
  other_params <- " "

  if (!is.null(icao24)) {
    icao24 <- icao24 %>%
      str_enclose(pad = "'") %>%
      stringr::str_c(collapse = ", ")
    other_params <- stringr::str_glue(
      other_params,
      " AND icao24 in ({ICAO24}) ",
      ICAO24 = icao24)
  }
  if (!is.null(bbox)) {
    other_params <- stringr::str_glue(
      other_params,
      " AND (({lon_min} <= lon AND lon <={lon_max}) AND ({lat_min} <= lat AND lat <={lat_max}))",
      lon_min = bbox["xmin"],
      lon_max = bbox["xmax"],
      lat_min = bbox["ymin"],
      lat_max = bbox["ymax"])
  }

  # [hadoop-1:21000] > describe state_vectors_data4;
  # +---------------+------------+-----------------------------+
  # | name          | type       | comment                     |
  # +---------------+------------+-----------------------------+
  # | time          | int        | Inferred from Parquet file. |
  # | icao24        | string     | Inferred from Parquet file. |
  # | lat           | double     | Inferred from Parquet file. |
  # | lon           | double     | Inferred from Parquet file. |
  # | velocity      | double     | Inferred from Parquet file. |
  # | heading       | double     | Inferred from Parquet file. |
  # | vertrate      | double     | Inferred from Parquet file. |
  # | callsign      | string     | Inferred from Parquet file. |
  # | onground      | boolean    | Inferred from Parquet file. |
  # | alert         | boolean    | Inferred from Parquet file. |
  # | spi           | boolean    | Inferred from Parquet file. |
  # | squawk        | string     | Inferred from Parquet file. |
  # | baroaltitude  | double     | Inferred from Parquet file. |
  # | geoaltitude   | double     | Inferred from Parquet file. |
  # | lastposupdate | double     | Inferred from Parquet file. |
  # | lastcontact   | double     | Inferred from Parquet file. |
  # | serials       | array<int> | Inferred from Parquet file. |
  # | hour          | int        |                             |
  # +---------------+------------+-----------------------------+
  cols <- readr::cols(
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

  query <- stringr::str_glue(
    "SELECT {COLUMNS} FROM state_vectors_data4 {OTHER_TABLES} ",
    "WHERE hour >= {WEFH} and hour < {TILH} ",
    # "and time >= {WEFT} and time < {WEFT} ",
    "{OTHER_PARAMS};",
    COLUMNS = paste(names(cols$cols), collapse = ", "),
    WEFH = wef,
    TILH = til,
    ICAO24 = icao24,
    OTHER_TABLES = "",
    OTHER_PARAMS = other_params)
  logger::log_debug('Impala query = {query}')

  impala_query(session, query, cols)
}


#' Get few state vector's attributes for a time period (listing who was present)
#'
#' @param session  SSH session to OSN Impala
#' @param icao24   (Optional) Single or vector of ICAO24 ICAO 24-bit addresses
#' @param wef      Start of period of interest (date or datetime)
#' @param til      (Optional) End of period of interest, if NULL wef_time + 1 day
#' @param bbox     (Optional) axis aligned bounding box like
#'                 `c(xmin, xmax, ymin, ymax)`
#'
#' @return a dataframe of state vectors with distinct icao24, callsign and hour
#' @export
#'
#' @examples
#' \dontrun{
#' session <- connect_osn("cucu", verbose = 2)
#' minimal_state_vector(
#'    session,
#'    icao24 = c("3c6589", "3c6757"),
#'    wef_time = "2019-04-22 00:00:00",
#'    til_time = "2019-04-22 10:00:00",
#'    bbox = c(xmin = 7.536746, xmax = 9.604390, ymin = 49.36732, ymax = 50.69920)
#' )
#' }
minimal_state_vector <- function(session,
                         icao24,
                         wef, til = NULL,
                         bbox = NULL
) {
  wef_time <- lubridate::as_datetime(wef)
  stopifnot(class(wef) %in% c("POSIXct", "POSIXt"))

  if (is.null(til)) {
    til <- wef + lubridate::days(1)
  } else {
    til <- lubridate::as_datetime(til)
    stopifnot(class(til) %in% c("POSIXct", "POSIXt"))
  }
  logger::log_debug('Input argument wef = {format(wef, "%Y-%m-%d %H:%M:%S")}')
  logger::log_debug('Input argument til = {format(til, "%Y-%m-%d %H:%M:%S")}')

  wef <- as.numeric(wef)
  til <- as.numeric(til)
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
      lon_min = bbox["xmin"],
      lon_max = bbox["xmax"],
      lat_min = bbox["ymin"],
      lat_max = bbox["ymax"])
  }

  query <- stringr::str_glue(
    "SELECT {COLUMNS} FROM state_vectors_data4 {OTHER_TABLES} ",
    "WHERE hour >= {WEFH} and hour < {TILH} ",
    # "and time >= {WEFT} and time < {WEFT} ",
    "{OTHER_PARAMS};",
    COLUMNS = "DISTINCT icao24, callsign, hour",
    WEFH = wef,
    TILH = til,
    ICAO24 = icao24,
    OTHER_TABLES = "",
    OTHER_PARAMS = other_params)
  logger::log_debug('Impala query = {query}')

  cols <- readr::cols(
    icao24        = readr::col_character(),
    callsign      = readr::col_character(),
    hour          = readr::col_integer()
  )
  impala_query(session, query, cols)
}


#' Get state vectors for arrivals at airport
#'
#' Retrive state vectors for all flights from flights table
#'
#' @param session SSH session to OSN Impala
#' @param apt ICAO ID of airport, i.e. "EDDF" for Frankfurt
#' @param wef (UTC) timestamp of With Effect From (included)
#' @param til (UTC) timestamp of TILl instant (excluded), if NULL
#'            if is interpreted as WEF + 1 day.
#' @param duration number of second back from `lastseen`
#'
#' @return data frame of state vector data of flights containing the following
#'         variables (see also OSN docs about
#'   \href{https://opensky-network.org/apidoc/rest.html#arrivals-by-airport}{Arrivals
#'    by Airport}):
#'    \tabular{lll}{
#'      \strong{Name}       \tab \strong{Description} \tab \strong{Type} \cr
#'      icao24              \tab ICAO 24-bit address \tab chr \cr
#'      callsign            \tab flight's callsign   \tab chr \cr
#'      estdepartureairport \tab Estimated departure airport \tab chr \cr
#'      estarrivalairport   \tab Estimated arrival airport   \tab chr \cr
#'      start               \tab Start of portion of trajectory: `min(firstseen, lastseen - duration)` \tab int \cr
#'      firstseen           \tab first seen by OpenSky Network (UNIX timestamp)\tab int \cr
#'      lastseen            \tab last seen by OpenSky Network (UNIX timestamp) \tab int \cr
#'      item.time           \tab position report's time (UNIX timestamp) \tab int \cr
#'      item.longitude      \tab position report's longitude (WSG84 decimal degrees)\tab dbl \cr
#'      item.latitude       \tab position report's latitude (WSG84 decimal degrees) \tab dbl \cr
#'      item.velocity       \tab ground speed of the aircraft (m/s) \tab dbl \cr
#'      item.heading        \tab true track in decimal degrees clockwise from north (north=0°) \tab dbl \cr
#'      item.vertrate       \tab vertical speed of the aircraft (m/s) \tab dbl \cr
#'      item.onground       \tab TRUE if the position was retrieved from a surface position report \tab lgl \cr
#'      item.baroaltitude   \tab position report's barometric altitude (m) \tab dbl \cr
#'      item.geoaltitude    \tab position report's GNSS (GPS) altitude (m) \tab dbl \cr
#'      item.hour           \tab position report's hour (UNIX timestamp) \tab int
#'    }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' session <- connect_osn("cucu", verbose = 2)
#' arrivals_state_vector(session, "EDDF", "2019-04-22 00:00:00", til=NULL)
#' }
arrivals_state_vector <- function(
  session,
  apt, wef, til = NULL,
  duration = 3600) {
  wef <- lubridate::as_datetime(wef)
  stopifnot(class(wef) %in% c("POSIXct", "POSIXt"))
  if (is.null(til)) {
    til <- wef + lubridate::days(1)
  } else {
    til <- lubridate::as_datetime(til)
    stopifnot(class(til) %in% c("POSIXct", "POSIXt"))
  }
  logger::log_trace('Input argument wef = {format(wef, "%Y-%m-%d %H:%M:%S")}')
  logger::log_trace('Input argument til = {format(til, "%Y-%m-%d %H:%M:%S")}')
  wef <- wef %>% as.integer()
  til <- til %>% as.integer()
  # floor to POSIX hour
  wefh <- wef - (wef %% 3600)
  tilh <- til - (til %% 3600)
  # floor to POSIX day
  wefd <- wefh - (wefh %% 86400)
  tild <- tilh - (tilh %% 86400)
  minimum <- wefh - duration

  # NOTE: less or equal (<=) is ESSENTIAL in WHERE clause for `day`
  query <- stringr::str_glue(
    "WITH fl AS (
      -- consider all the arrivals at APT on the interval [WEF, TIL) of interest
      SELECT
        icao24, callsign, day,
        firstseen, lastseen,
        if(lastseen - {DURATION} < firstseen, firstseen, lastseen - {DURATION}) start,
        estdepartureairport, estarrivalairport
      FROM
        flights_data4
      WHERE
        estarrivalairport LIKE '%{APT}%'
        AND (({WEF}  <= lastseen) AND (lastseen <  {TIL}))
        AND (({WEFD} <= day)      AND (day      <= {TILD}))
      )
  -- get all portions of state vector within the interval of interest
  SELECT
    sv.icao24, sv.callsign,
    fl.estdepartureairport, fl.estarrivalairport,
    fl.start, fl.firstseen, fl.lastseen,
    sv.time, sv.lon longitude, sv.lat latitude, sv.velocity, sv.heading, sv.vertrate,
    sv.onground, sv.baroaltitude, sv.geoaltitude,
    sv.hour
  FROM
    state_vectors_data4 sv, fl
  WHERE
    -- IMPORTANT to reduce memory (file scan) for query
    -- we use as MINIMUM the (floor of) HOUR for WEF minus DURATION
    -- and as MAXIMUN the TIL hour
    (({MINIMUM} <= sv.hour) AND (sv.hour < {TILH}))
    -- retrieve only the sv portion from START to LASTSEEN
    AND ((fl.start <= sv.time) AND (sv.time <= fl.lastseen))
    -- olny for the relevant arrivals as from FL
    AND sv.icao24 = fl.icao24;",
    APT = apt,
    WEF = wef,
    WEFH = wefh,
    WEFD = wefd,
    TIL = til,
    TILH = tilh,
    TILD = tild,
    DURATION = duration,
    MINIMUM = minimum)

  logger::log_debug('Impala query = {query}')

  cols <- readr::cols(
    icao24              = readr::col_character(),
    callsign            = readr::col_character(),
    estdepartureairport = readr::col_character(),
    estarrivalairport   = readr::col_character(),
    start               = readr::col_double(),
    firstseen           = readr::col_double(),
    lastseen            = readr::col_double(),
    time                = readr::col_double(),
    longitude           = readr::col_double(),
    latitude            = readr::col_double(),
    velocity            = readr::col_double(),
    heading             = readr::col_double(),
    vertrate            = readr::col_double(),
    onground            = readr::col_logical(),
    baroaltitude        = readr::col_double(),
    geoaltitude         = readr::col_double(),
    hour                = readr::col_double()
  )
  impala_query(session, query, cols)
}
