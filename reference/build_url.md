# URL of one NASA POWER request for a point

URL of one NASA POWER request for a point

## Usage

``` r
build_url(
  latitude,
  longitude,
  parameters,
  temporal_resolution = "daily",
  community = "RE",
  start = NULL,
  end = NULL
)
```

## Arguments

- latitude, longitude:

  Coordinates of the point (decimal degrees, WGS84).

- parameters:

  NASA POWER parameters, e.g. `c("T2M", "PRECTOTCORR")`.

- temporal_resolution:

  `"daily"`, `"monthly"` or `"climatology"`.

- community:

  `"RE"`, `"AG"` or `"SB"`.

- start, end:

  Dates as `"YYYYMMDD"` (for `"monthly"` only the year is used; not used
  for `"climatology"`).

## Value

A character string with the URL.

## Examples

``` r
build_url(20.97, -89.62, c("T2M", "PRECTOTCORR"), "daily", "AG",
          "20240101", "20241231")
#> [1] "https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,PRECTOTCORR&community=AG&longitude=-89.62&latitude=20.97&format=JSON&start=20240101&end=20241231"
```
