# climex 0.1.0

* First version, the R counterpart of climex-py 0.2.0.
* `download_nasa_power_data()`: NASA POWER for a set of points, one request
  per point with all the period, up to five at a time, with retries and a
  second round for the points that fail; a point whose CSV already has the
  period is not asked again (resume and weekly updates); the result has one
  row per point and date, without duplicates, and a final report.
* `municipalities()`: one point for each of the 2,465 municipalities of
  Mexico (INEGI 2019), by state or for all the country.
* `centroids()`, `build_url()`, `parse_response()`.
