#' Create an ssh session to OpenSky Network’s Impala shell.
#'
#' @param usr     user account
#' @inheritParams ssh::ssh_connect
#'
#' @return an SSH session
#' @export
#'
#' @examples
#' \dontrun{
#' session <- connect_osn("cucu", verbose = 2)
#' }
connect_osn <- function(usr, passwd = askpass, verbose = FALSE) {
  host <- stringr::str_glue("{usr}@data.opensky-network.org:2230", usr = usr)
  ssh::ssh_connect(host, passwd = passwd, verbose = verbose)
}

#' Disconnect from OpenSky Network’s Impala shell.
#'
#' @inheritParams ssh::ssh_disconnect
#'
#' @return an SSH session
#' @export
#'
#' @examples
#' \dontrun{
#' session <- connect_osn("cucu", verbose = 2)
#' disconnect_osn(session)
#' }
disconnect_osn <- function(session) {
  ssh::ssh_disconnect(session)
}
