.api <- "https://power.larc.nasa.gov/api/temporal"
.temporal <- c("daily", "monthly", "climatology")
.communities <- c("RE", "AG", "SB")

#' URL of one NASA POWER request for a point
#'
#' @param latitude,longitude Coordinates of the point (decimal degrees, WGS84).
#' @param parameters NASA POWER parameters, e.g. `c("T2M", "PRECTOTCORR")`.
#' @param temporal_resolution `"daily"`, `"monthly"` or `"climatology"`.
#' @param community `"RE"`, `"AG"` or `"SB"`.
#' @param start,end Dates as `"YYYYMMDD"` (for `"monthly"` only the year is
#'   used; not used for `"climatology"`).
#' @return A character string with the URL.
#' @export
#' @examples
#' build_url(20.97, -89.62, c("T2M", "PRECTOTCORR"), "daily", "AG",
#'           "20240101", "20241231")
build_url <- function(latitude, longitude, parameters,
                      temporal_resolution = "daily", community = "RE",
                      start = NULL, end = NULL) {
    if (!temporal_resolution %in% .temporal) {
        stop("temporal_resolution must be one of: ", paste(.temporal, collapse = ", "), call. = FALSE)
    }
    if (!community %in% .communities) {
        stop("community must be one of: ", paste(.communities, collapse = ", "), call. = FALSE)
    }
    if (length(parameters) == 0) stop("give at least one parameter", call. = FALSE)
    url <- sprintf("%s/%s/point?parameters=%s&community=%s&longitude=%s&latitude=%s&format=JSON",
                   .api, temporal_resolution, paste(parameters, collapse = ","),
                   community, longitude, latitude)
    if (temporal_resolution == "daily") {
        url <- paste0(url, "&start=", start, "&end=", end)
    } else if (temporal_resolution == "monthly") {
        url <- paste0(url, "&start=", substr(start, 1, 4), "&end=", substr(end, 1, 4))
    }
    url
}

#' Table from the JSON answer of NASA POWER
#'
#' @param js The JSON answer, as text or as the list of [jsonlite::fromJSON()].
#' @inheritParams build_url
#' @return A data frame with one row per day (`date`), year and month
#'   (`year`, `month`; the annual value 13 is dropped) or period (`period`),
#'   and one column per parameter. Missing values (-999) are `NA`.
#' @export
parse_response <- function(js, temporal_resolution = "daily") {
    if (is.character(js)) js <- jsonlite::fromJSON(js, simplifyVector = FALSE)
    par <- js$properties$parameter
    if (is.null(par) || length(par) == 0) {
        msg <- js$messages %||% js$errors %||% js$message %||% "no data"
        stop("NASA POWER: ", paste(unlist(msg), collapse = "; "), call. = FALSE)
    }
    key <- names(par[[1]])
    df <- as.data.frame(lapply(par, function(v) {
        x <- as.numeric(unlist(v[key]))
        x[x == -999] <- NA
        x
    }), check.names = FALSE)
    if (temporal_resolution == "daily") {
        df <- cbind(date = as.Date(key, "%Y%m%d"), df)
    } else if (temporal_resolution == "monthly") {
        df <- cbind(year = as.integer(substr(key, 1, 4)), month = as.integer(substr(key, 5, 6)), df)
        df <- df[df$month <= 12, , drop = FALSE]
    } else {
        df <- cbind(period = key, df)
    }
    rownames(df) <- NULL
    df
}

`%||%` <- function(a, b) if (is.null(a)) b else a
