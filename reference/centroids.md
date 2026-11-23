# Points inside polygons, to download their climate

Uses the representative point of each polygon
([`sf::st_point_on_surface()`](https://r-spatial.github.io/sf/reference/geos_unary.html)),
which is always inside it; the centroid of a curved municipality can
fall outside.

## Usage

``` r
centroids(x, id_col)
```

## Arguments

- x:

  An `sf` object with polygons, e.g. the AGEB or localities of rgeomex.

- id_col:

  Column with the identifier of each polygon (e.g. `"CVEGEO"`).

## Value

A data frame with `id_col`, `latitude` and `longitude` (WGS84), ready
for
[`download_nasa_power_data()`](https://fdzul.github.io/climex/reference/download_nasa_power_data.md).
