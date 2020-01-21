#' Create an ssh session to OpenSky Network’s Impala shell.
#'
#' @param usr     user account
#' @param port    port to connect to
#' @inheritParams ssh::ssh_connect
#'
#' @return an SSH session
#' @export
#'
#' @examples
#' \dontrun{
#' # connect directly to OSN
#' session <- osn_connect("cucu", verbose = 2)
#'
#' # connect via SSH port forwarding
#' session <- oan_connect_osn(
#'   usr = Sys.getenv("OSN_USER"),
#'   passwd = Sys.getenv("OSN_PASSWORD"),
#'   port = 6666,
#'   host = "localhost"
#' )
#' }
osn_connect <- function(usr, passwd = askpass,
                        host = "data.opensky-network.org", port = 2230,
                        verbose = FALSE) {
  fullhost <- stringr::str_glue("{usr}@{host}:{port}")
  ssh::ssh_connect(fullhost, passwd = passwd, verbose = verbose)
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
#' session <- osn_connect("cucu", verbose = 2)
#' osn_disconnect(session)
#' }
osn_disconnect <- function(session) {
  ssh::ssh_disconnect(session)
}
