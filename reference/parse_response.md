# Table from the JSON answer of NASA POWER

Table from the JSON answer of NASA POWER

## Usage

``` r
parse_response(js, temporal_resolution = "daily")
```

## Arguments

- js:

  The JSON answer, as text or as the list of
  [`jsonlite::fromJSON()`](https://rdrr.io/pkg/jsonlite/man/fromJSON.html).

- temporal_resolution:

  `"daily"`, `"monthly"` or `"climatology"`.

## Value

A data frame with one row per day (`date`), year and month (`year`,
`month`; the annual value 13 is dropped) or period (`period`), and one
column per parameter. Missing values (-999) are `NA`.
