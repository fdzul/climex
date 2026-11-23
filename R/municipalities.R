#' One point per municipality of Mexico (INEGI 2019), all or by state
#'
#' The table comes with the package (2,465 municipalities), so no download is
#' needed; it is ready for [download_nasa_power_data()] with
#' `id_col = "CVEGEO"`.
#'
#' @param cve_edo State code(s) (`"31"`, `31` or `c("14", "31")`). `NULL`
#'   returns the 2,465 municipalities of the country.
#' @param point `"inside"` (default) uses a point that is always inside the
#'   municipality (its representative point); `"centroid"` uses the geometric
#'   centroid, which falls outside 67 curved or fragmented municipalities
#'   (column `centroid_inside`).
#' @return A data frame with `CVEGEO`, `CVE_ENT`, `state`, `CVE_MUN`,
#'   `municipality`, `latitude`, `longitude` (WGS84), `centroid_lat`,
#'   `centroid_lon`, `centroid_inside` and `area_km2`.
#' @export
#' @examples
#' yuc <- municipalities("31")        # the 106 municipalities of Yucatán
#' head(yuc[, c("CVEGEO", "municipality", "latitude", "longitude")])
#' nrow(municipalities())             # all the country
municipalities <- function(cve_edo = NULL, point = c("inside", "centroid")) {
    point <- match.arg(point)
    f <- system.file("extdata", "municipalities.csv", package = "climex")
    df <- utils::read.csv(f, colClasses = c(CVEGEO = "character", CVE_ENT = "character",
                                            CVE_MUN = "character"),
                          encoding = "UTF-8")
    if (!is.null(cve_edo)) {
        codes <- formatC(as.integer(cve_edo), width = 2, flag = "0")
        bad <- setdiff(codes, df$CVE_ENT)
        if (length(bad)) stop("state code(s) not valid (01-32): ", paste(bad, collapse = ", "), call. = FALSE)
        df <- df[df$CVE_ENT %in% codes, , drop = FALSE]
    }
    if (point == "centroid") {
        df$latitude <- df$centroid_lat
        df$longitude <- df$centroid_lon
    }
    rownames(df) <- NULL
    df
}

#' Points inside polygons, to download their climate
#'
#' Uses the representative point of each polygon (`sf::st_point_on_surface()`),
#' which is always inside it; the centroid of a curved municipality can fall
#' outside.
#'
#' @param x An `sf` object with polygons, e.g. the AGEB or localities of
#'   rgeomex.
#' @param id_col Column with the identifier of each polygon (e.g. `"CVEGEO"`).
#' @return A data frame with `id_col`, `latitude` and `longitude` (WGS84),
#'   ready for [download_nasa_power_data()].
#' @export
centroids <- function(x, id_col) {
    if (!requireNamespace("sf", quietly = TRUE)) stop("centroids() needs the package sf", call. = FALSE)
    g <- if (isTRUE(sf::st_is_longlat(x))) sf::st_transform(x, 6372) else x
    p <- sf::st_transform(suppressWarnings(sf::st_point_on_surface(sf::st_geometry(g))), 4326)
    xy <- sf::st_coordinates(p)
    out <- data.frame(id = sf::st_drop_geometry(x)[[id_col]],
                      latitude = round(xy[, "Y"], 5), longitude = round(xy[, "X"], 5))
    names(out)[1] <- id_col
    out
}
