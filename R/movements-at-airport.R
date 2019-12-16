#' Get arrival tracks at airport
#'
#' NOTE: flight data is simplified in OSN, e.g. altitudes are discretized to multiples of 1000 feet.
#'
#' @param session SSH session to OSN Impala
#' @param apt ICAO ID of airport, i.e. "EDDF" for Frankfurt
#' @param wef (UTC) timestamp of With Effect From (included)
#' @param til (UTC) timestamp of TILl instant (excluded), if NULL
#'            if is interpreted as WEF + 1 day.
#'
#' @return data frame of flight and track data containing the following
#'         variables (see also OSN docs about
#'   \href{https://opensky-network.org/apidoc/rest.html#arrivals-by-airport}{Arrivals
#'    by Airport}):
#'    \tabular{lll}{
#'      \strong{Name}       \tab \strong{Description} \tab \strong{Type} \cr
#'      icao24              \tab ICAO 24-bit address \tab chr \cr
#'      callsign            \tab flight's callsign   \tab chr \cr
#'      day                 \tab flight's day  \tab int \cr
#'      firstseen           \tab first seen by OpenSky Network (UNIX timestamp)\tab int \cr
#'      lastseen            \tab last seen by OpenSky Network (UNIX timestamp) \tab int \cr
#'      estdepartureairport \tab Estimated departure airport \tab chr \cr
#'      estarrivalairport   \tab Estimated arrival airport   \tab chr \cr
#'      item.time           \tab position report's time (UNIX timestamp) \tab int \cr
#'      item.longitude      \tab position report's longitude (WSG84 decimal degrees)\tab dbl \cr
#'      item.latitude       \tab position report's latitude (WSG84 decimal degrees) \tab dbl \cr
#'      item.altitude       \tab position report's barometric altitude (meters) \tab dbl \cr
#'      item.heading        \tab true track in decimal degrees clockwise from north (north=0°) \tab dbl \cr
#'      item.onground       \tab TRUE if the position was retrieved from a surface position report \tab lgl
#'    }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' session <- connect_osn("cucu", verbose = 2)
#' arrivals(session, "EDDF", "2019-04-22 00:00:00", til=NULL)
#' }
arrivals <- function(session, apt, wef, til=NULL) {
  wef <- lubridate::as_datetime(wef)
  if (is.null(til)) {
    til <- wef + lubridate::days(1)
  } else {
    til <- lubridate::as_datetime(til)
  }
  wef <- wef %>% as.integer()
  til <- til %>% as.integer()

  # SELECT
  # icao24,
  # callsign,
  # firstseen,
  # latseen,
  # from_unixtime(day, 'yyyy-MM-dd') as day,
  # estdepartureairport,
  # estarrivalairport,
  # track.item.time,
  # track.item.longitude,
  # track.item.latitude,
  # track.item.altitude,
  # track.item.heading,
  # track.item.onground
  # FROM
  # flights_data4,
  # flights_data4.track
  # WHERE
  # estarrivalairport LIKE '%EDDF%'
  # AND ( day >= 1541894400 AND day < 1541980800)
  # -- LIMIT 7
  # ;

  columns <- c(
    "icao24",
    "callsign",
    "day",
    "firstseen",
    "lastseen",
    "estdepartureairport",
    "estarrivalairport",
    "track.item.time",
    "track.item.longitude",
    "track.item.latitude",
    "track.item.altitude",
    "track.item.heading",
    "track.item.onground"
  )

  tables <- c(
    "flights_data4",
    "flights_data4.track"
  )
  query <- stringr::str_glue(
    "SELECT {COLUMNS} ",
    "FROM {TABLES} ",
    "WHERE ",
    "estdepartureairport like '%{APT}%' ",
    " and firstseen >= {WEF} ",
    " and firstseen <  {TIL};",
    COLUMNS = stringr::str_c(columns, collapse = ","),
    TABLES = stringr::str_c(tables, collapse = ","),
    APT = apt,
    WEF = wef,
    TIL = til)
  cmd <-stringr::str_glue("-q {query}", query = query)
  lines <- ssh::ssh_exec_internal(session, cmd) %>%
    { rawToChar(.$stdout)} %>%
    stringi::stri_split_lines(omit_empty = TRUE)

  # create an empty dataframe to return in case of empty query
  values <- tibble::tibble(
    icao24              = character(),
    callsign            = character(),
    day                 = integer(),
    firstseen           = integer(),
    lastseen            = integer(),
    estdepartureairport = character(),
    estarrivalairport   = character(),
    item.time           = integer(),
    item.longitude      = double(),
    item.latitude       = double(),
    item.altitude       = double(),
    item.heading        = double(),
    item.onground       = logical()
  )
  if (length(lines) >= 1) {
    lines <- lines %>%
      purrr::flatten_chr() %>%
      # match all lines starting w/ '|'
      stringr::str_subset(pattern = "^\\|")
    if (length(lines) >= 1) {
      lines <- lines %>%
        # remove first and last field separator, '|'
        stringr::str_replace_all("^[|](.+)[|]$", "\\1") %>%
        stringr::str_replace_all("\\s*\\|\\s*", ",") %>%
        stringr::str_trim(side = "both")

      # remove duplicated heading (with column names)
      values_to_parse <- lines[!duplicated(lines)]
      cols <- readr::cols(
        icao24              = readr::col_character(),
        callsign            = readr::col_character(),
        day                 = readr::col_integer(),
        firstseen           = readr::col_integer(),
        lastseen            = readr::col_integer(),
        estdepartureairport = readr::col_character(),
        estarrivalairport   = readr::col_character(),
        item.time           = readr::col_integer(),
        item.longitude      = readr::col_double(),
        item.latitude       = readr::col_double(),
        item.altitude       = readr::col_double(),
        item.heading        = readr::col_double(),
        item.onground       = readr::col_logical()
      )
      values <- values_to_parse %>%
        readr::read_csv(
          na = c("", "NULL"),
          col_types = cols) %>%
        janitor::clean_names()
    }
  }
  values
}

#' Get departures from airport
#'
#' NOTE: flight data is simplified in OSN, e.g. altitudes are discretized to multiples of 1000 feet.
#'
#' @param session SSH session to OSN Impala
#' @param apt ICAO ID of airport, i.e. "EDDF" for Frankfurt
#' @param wef Start of period of interest
#' @param til End of period of interest
#'
#' @inherit arrivals return
#' @export
#'
#' @examples
#' \dontrun{
#' session <- connect_osn("cucu", verbose = 2)
#' depurtures(session, "EDDF", "2019-04-22 00:00:00", til=NULL)
#' }
departures <- function(session, apt, wef, til=NULL) {
  wef <- lubridate::as_datetime(wef)
  if (is.null(til)) {
    til <- wef + lubridate::days(1)
  } else {
    til <- lubridate::as_datetime(til)
  }
  wef <- wef %>% as.integer()
  til <- til %>% as.integer()

  columns <- c(
    "icao24",
    "callsign",
    "day",
    "firstseen",
    "lastseen",
    "estdepartureairport",
    "estarrivalairport",
    "track.item.time",
    "track.item.longitude",
    "track.item.latitude",
    "track.item.altitude",
    "track.item.heading",
    "track.item.onground"
  )

  tables <- c(
    "flights_data4",
    "flights_data4.track"
  )
  query <- stringr::str_glue(
    "SELECT {COLUMNS} ",
    "FROM {TABLES} ",
    "WHERE ",
    "estdepartureairport like '%{APT}%' ",
    " and firstseen >= {WEF} ",
    " and firstseen <  {TIL};",
    COLUMNS = stringr::str_c(columns, collapse = ","),
    TABLES = stringr::str_c(tables, collapse = ","),
    APT = apt,
    WEF = wef,
    TIL = til)

  cmd <- stringr::str_glue("-q {query}", query = query)
  lines <- ssh::ssh_exec_internal(
    session,
    cmd) %>%
    { rawToChar(.$stdout)} %>%
    stringi::stri_split_lines() %>%
    purrr::flatten_chr() %>%
    # match all lines starting w/ '|'
    stringr::str_subset(pattern = "^\\|") %>%
    # remove first and last field separator, '|'
    stringr::str_replace_all("^[|](.+)[|]$", "\\1") %>%
    stringr::str_replace_all("\\s*\\|\\s*", ",") %>%
    stringr::str_trim(side = "both")

  # remove duplicated heading (with column names)
  values_to_parse <- lines[!duplicated(lines)]
  cols <- readr::cols(
    icao24 = readr::col_character(),
    callsign = readr::col_character(),
    day = readr::col_integer(),
    firstseen = readr::col_integer(),
    lastseen = readr::col_integer(),
    estdepartureairport = readr::col_character(),
    estarrivalairport = readr::col_character(),
    item.time = readr::col_integer(),
    item.longitude = readr::col_double(),
    item.latitude = readr::col_double(),
    item.altitude = readr::col_double(),
    item.heading = readr::col_double(),
    item.onground = readr::col_logical()
  )
  values <- values_to_parse %>%
    readr::read_csv(
      na = c("", "NULL"),
      col_types = cols) %>%
    janitor::clean_names()
  values
}


# flights_data4
#   +----------------------------------+----------------------+
#   | name                             | type                 |
#   +----------------------------------+----------------------+
#   | icao24                           | string               |
#   | firstseen                        | int                  |
#   | estdepartureairport              | string               |
#   | lastseen                         | int                  |
#   | estarrivalairport                | string               |
#   | callsign                         | string               |
#   | track                            | array<struct<        |
#   |                                  |   time:int,          |
#   |                                  |   latitude:double,   |
#   |                                  |   longitude:double,  |
#   |                                  |   altitude:double,   |
#   |                                  |   heading:float,     |
#   |                                  |   onground:boolean   |
#   |                                  | >>                   |
#   | serials                          | array<int>           |
#   | estdepartureairporthorizdistance | int                  |
#   | estdepartureairportvertdistance  | int                  |
#   | estarrivalairporthorizdistance   | int                  |
#   | estarrivalairportvertdistance    | int                  |
#   | departureairportcandidatescount  | int                  |
#   | arrivalairportcandidatescount    | int                  |
#   | otherdepartureairportcandidates  | array<struct<        |
#   |                                  |   icao:string,       |
#   |                                  |   horizdistance:int, |
#   |                                  |   vertdistance:int   |
#   |                                  | >>                   |
#   | otherarrivalairportcandidates    | array<struct<        |
#   |                                  |   icao:string,       |
#   |                                  |   horizdistance:int, |
#   |                                  |   vertdistance:int   |
#   |                                  | >>                   |
#   | day                              | int                  |
#   +----------------------------------+----------------------+
