# Download NASA POWER climate data for the points of a table

One request per point with all the period, up to five at a time. Each
point is saved as `<identifier>.csv` in `output_folder`; a point whose
CSV already has all the period (with data in every row) is not
downloaded again, so an interrupted download can be run again and
continues where it stopped, and with a later `end_date` (an update) the
point is downloaded again. Busy answers of the API (429, 5xx) and
network errors are retried; the points that still fail get a second
round at the end.

## Usage

``` r
download_nasa_power_data(
  df,
  lat_col = "latitude",
  lon_col = "longitude",
  start_date = "20150101",
  end_date = "20150305",
  parameters = c("T2M", "T2M_MAX", "T2M_MIN"),
  temporal_resolution = "daily",
  community = "RE",
  processes = 5,
  output_folder = "./nasa_power_data",
  start_col = NULL,
  end_col = NULL,
  id_col = NULL,
  return_consolidated = TRUE,
  overwrite = FALSE,
  verbose = TRUE
)
```

## Arguments

- df:

  Data frame with one row per point, e.g.
  [`municipalities()`](https://fdzul.github.io/climex/reference/municipalities.md).

- lat_col, lon_col:

  Columns with the coordinates (decimal degrees, WGS84).

- start_date, end_date:

  Period as `"YYYYMMDD"`; for `"monthly"` only the year is used.

- parameters:

  NASA POWER parameters. Temperature: `T2M`, `T2M_MAX`, `T2M_MIN`,
  `T2M_RANGE`, `T2MDEW`, `T2MWET`, `TS`; precipitation: `PRECTOTCORR`;
  humidity: `RH2M`, `QV2M`; wind: `WS2M`, `WS10M`; radiation:
  `ALLSKY_SFC_SW_DWN`; pressure: `PS`. Up to 20 per request.

- temporal_resolution:

  `"daily"`, `"monthly"` or `"climatology"`.

- community:

  `"RE"` (renewable energy), `"AG"` (agroclimatology) or `"SB"`
  (sustainable buildings); it changes the units of some parameters.

- processes:

  Simultaneous requests (NASA asks for at most 5).

- output_folder:

  Folder for the CSV of each point.

- start_col, end_col:

  Optional columns with a different period for each point.

- id_col:

  Optional column with the identifier of each point (else `idx_<row>`);
  its values must be unique.

- return_consolidated:

  If `TRUE`, return the data of all the points in one table; if `FALSE`,
  the result of each download.

- overwrite:

  Download again every point, even if its CSV already has the period.

- verbose:

  Show the progress and the final report.

## Value

A data frame with `identifier`, `latitude`, `longitude`, the date
(`date`; `year` and `month`; or `period`) and one column per parameter,
only for the period requested and with one row per point and date,
sorted by point and date. Missing values (-999 in NASA POWER) are `NA`.

## Examples

``` r
if (FALSE) { # \dontrun{
yuc <- municipalities("31")
clima <- download_nasa_power_data(yuc, id_col = "CVEGEO",
                                  parameters = c("T2M", "PRECTOTCORR"),
                                  start_date = "20240101", end_date = "20241231",
                                  community = "AG")
} # }
```
