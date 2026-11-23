# Getting started with climex

The download chunks are not run when the package is built (they need the
NASA POWER API); the municipalities come with the package.

``` r

library(climex)
```

## Municipalities

``` r

yuc <- municipalities("31")
nrow(yuc)
#> [1] 106
head(yuc[, c("CVEGEO", "municipality", "latitude", "longitude", "area_km2")])
#>   CVEGEO municipality latitude longitude area_km2
#> 1  31001        Abalá 20.65865 -89.65313   290.24
#> 2  31002      Acanceh 20.82194 -89.46736   136.06
#> 3  31003         Akil 20.25901 -89.34417    75.87
#> 4  31004         Baca 21.13258 -89.39758   107.48
#> 5  31005       Bokobá 20.99497 -89.17299    71.06
#> 6  31006     Buctzotz 21.21079 -88.65766   648.29
table(municipalities()$centroid_inside)
#> 
#> False  True 
#>    67  2398
```

By default the point of each municipality is always inside it; with
`point = "centroid"` the geometric centroid is used, which falls outside
67 curved or fragmented municipalities.

## Download

``` r

clima <- download_nasa_power_data(
    yuc, id_col = "CVEGEO",
    parameters = c("T2M", "T2M_MAX", "T2M_MIN", "PRECTOTCORR", "RH2M"),
    start_date = "20240101", end_date = "20241231",
    temporal_resolution = "daily", community = "AG",
    output_folder = "nasa_power_yucatan")
#> 106 points; parameters: T2M, T2M_MAX, T2M_MIN, PRECTOTCORR, RH2M; daily; community AG; 5 processes
#> 106/106 points downloaded in 21.4 s
#> 38,796 rows; 106 points; no duplicates
#> complete: 106/106 points with all the period
```

| `temporal_resolution` | Rows | Period |
|----|----|----|
| `"daily"` | one per day (`date`) | `start_date` to `end_date` (`"YYYYMMDD"`) |
| `"monthly"` | one per year and month (`year`, `month`) | the years of `start_date` and `end_date` |
| `"climatology"` | one per month and the annual mean (`period`) | long-term average |

## Weekly update

Run the same call with a later `end_date` on the same `output_folder`:
the points whose CSV already has the period are skipped, the rest are
asked again, and the CSV are replaced (never appended), so nothing is
duplicated. Days not yet published by NASA POWER come as `NA` and are
asked again in the next run.

``` r

end <- format(Sys.Date() - 4, "%Y%m%d")
clima <- download_nasa_power_data(municipalities(), id_col = "CVEGEO",
                                  parameters = c("T2M", "RH2M", "PRECTOTCORR"),
                                  start_date = paste0(format(Sys.Date(), "%Y"), "0101"),
                                  end_date = end, community = "AG",
                                  output_folder = "nasa_power_mx")
```

## Other points

For polygons, e.g. the AGEB of a city from rgeomex,
[`centroids()`](https://fdzul.github.io/climex/reference/centroids.md)
gives a point inside each one:

``` r

ageb <- rgeomex::extract_ageb(locality = "Merida", cve_edo = "31")$ageb
pts <- centroids(ageb, id_col = "CVEGEO")
clima <- download_nasa_power_data(pts, id_col = "CVEGEO", parameters = "T2M",
                                  start_date = "20240101", end_date = "20241231")
```
