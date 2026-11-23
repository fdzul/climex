# One point per municipality of Mexico (INEGI 2019), all or by state

The table comes with the package (2,465 municipalities), so no download
is needed; it is ready for
[`download_nasa_power_data()`](https://fdzul.github.io/climex/reference/download_nasa_power_data.md)
with `id_col = "CVEGEO"`.

## Usage

``` r
municipalities(cve_edo = NULL, point = c("inside", "centroid"))
```

## Arguments

- cve_edo:

  State code(s) (`"31"`, `31` or `c("14", "31")`). `NULL` returns the
  2,465 municipalities of the country.

- point:

  `"inside"` (default) uses a point that is always inside the
  municipality (its representative point); `"centroid"` uses the
  geometric centroid, which falls outside 67 curved or fragmented
  municipalities (column `centroid_inside`).

## Value

A data frame with `CVEGEO`, `CVE_ENT`, `state`, `CVE_MUN`,
`municipality`, `latitude`, `longitude` (WGS84), `centroid_lat`,
`centroid_lon`, `centroid_inside` and `area_km2`.

## Examples

``` r
yuc <- municipalities("31")        # the 106 municipalities of Yucatán
head(yuc[, c("CVEGEO", "municipality", "latitude", "longitude")])
#>   CVEGEO municipality latitude longitude
#> 1  31001        Abalá 20.65865 -89.65313
#> 2  31002      Acanceh 20.82194 -89.46736
#> 3  31003         Akil 20.25901 -89.34417
#> 4  31004         Baca 21.13258 -89.39758
#> 5  31005       Bokobá 20.99497 -89.17299
#> 6  31006     Buctzotz 21.21079 -88.65766
nrow(municipalities())             # all the country
#> [1] 2465
```
