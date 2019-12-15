
<!-- README.md is generated from README.Rmd. Please edit that file -->

# osn

<!-- badges: start -->

<!-- badges: end -->

The goal of osn is to provide access to OpeSky Network historical data.

## Installation

Once published, you will be able to install the released version of osn
from [CRAN](https://CRAN.R-project.org) with:

``` r
install.packages("osn")
```

And the development version from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("espinielli/osn")
```

## Configuration

`osn` uses the `logger` package to write log messages to the console by
default with the following log level standards:

    TRACE writes the query results in the file `query_output.txt`.
    DEBUG logs the Impala query being submitted.
    INFO currently not used
    WARN currently not used
    ERROR currently not used
    FATAL currently not used

If you want to update the default log level threshold, use the package
name for the namespace argument of `log_threshold` from the `logger`
package, e.g. to enable all log messages:

``` r
library(logger)
log_threshold(TRACE, namespace = 'osn')
```

## Example

The following example shows how to retieve one hour worth of State
Vector data within a bounding box\[1\] around Frankfurt airport for an
hour interval, 09:00 - 10:00 UTC, on Jan 1, 2019:

``` r
library(osn)

# EDDF
state_vector(
  session,
  wef_time = "2019-01-01 09:00:00",
  til_time = "2019-01-01 10:00:00",
  bbox = c(7.553013, 49.378819,  9.585482, 50.688044),
  icao24 = NULL
)
#> # A tibble: 108,470 x 17
     time icao24   lat   lon velocity heading vertrate callsign onground alert spi   squawk
    <int> <fct>  <dbl> <dbl>    <dbl>   <dbl>    <dbl> <fct>    <lgl>    <lgl> <lgl> <fct> 
 1 1.55e9 06a063  50.4  7.56     272.    101.        0 QTR006   FALSE    FALSE FALSE 4734  
 2 1.55e9 06a063  50.4  7.56     272.    101.        0 QTR006   FALSE    FALSE FALSE 4734  
 3 1.55e9 06a063  50.4  7.56     272.    101.        0 QTR006   FALSE    FALSE FALSE 4734  
 4 1.55e9 06a063  50.4  7.57     272.    101.        0 QTR006   FALSE    FALSE FALSE 4734  
# … with 108,466 more rows, and 5 more variables: baroaltitude <dbl>, geoaltitude <dbl>,
#   lastposupdate <dbl>, lastcontact <dbl>, hour <int>
```

1.  40 nautical miles around EDDF’s reference point at E8°34.23’,
    N50°2.00’
