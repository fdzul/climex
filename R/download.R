#' Download NASA POWER climate data for the points of a table
#'
#' One request per point with all the period, up to five at a time. Each
#' point is saved as `<identifier>.csv` in `output_folder`; a point whose CSV
#' already has all the period (with data in every row) is not downloaded
#' again, so an interrupted download can be run again and continues where it
#' stopped, and with a later `end_date` (an update) the point is downloaded
#' again. Busy answers of the API (429, 5xx) and network errors are retried;
#' the points that still fail get a second round at the end.
#'
#' @param df Data frame with one row per point, e.g. [municipalities()].
#' @param lat_col,lon_col Columns with the coordinates (decimal degrees, WGS84).
#' @param start_date,end_date Period as `"YYYYMMDD"`; for `"monthly"` only the
#'   year is used.
#' @param parameters NASA POWER parameters. Temperature: `T2M`, `T2M_MAX`,
#'   `T2M_MIN`, `T2M_RANGE`, `T2MDEW`, `T2MWET`, `TS`; precipitation:
#'   `PRECTOTCORR`; humidity: `RH2M`, `QV2M`; wind: `WS2M`, `WS10M`;
#'   radiation: `ALLSKY_SFC_SW_DWN`; pressure: `PS`. Up to 20 per request.
#' @param temporal_resolution `"daily"`, `"monthly"` or `"climatology"`.
#' @param community `"RE"` (renewable energy), `"AG"` (agroclimatology) or
#'   `"SB"` (sustainable buildings); it changes the units of some parameters.
#' @param processes Simultaneous requests (NASA asks for at most 5).
#' @param output_folder Folder for the CSV of each point.
#' @param start_col,end_col Optional columns with a different period for
#'   each point.
#' @param id_col Optional column with the identifier of each point (else
#'   `idx_<row>`); its values must be unique.
#' @param return_consolidated If `TRUE`, return the data of all the points in
#'   one table; if `FALSE`, the result of each download.
#' @param overwrite Download again every point, even if its CSV already has
#'   the period.
#' @param verbose Show the progress and the final report.
#' @return A data frame with `identifier`, `latitude`, `longitude`, the date
#'   (`date`; `year` and `month`; or `period`) and one column per parameter,
#'   only for the period requested and with one row per point and date,
#'   sorted by point and date. Missing values (-999 in NASA POWER) are `NA`.
#' @export
#' @examples
#' \dontrun{
#' yuc <- municipalities("31")
#' clima <- download_nasa_power_data(yuc, id_col = "CVEGEO",
#'                                   parameters = c("T2M", "PRECTOTCORR"),
#'                                   start_date = "20240101", end_date = "20241231",
#'                                   community = "AG")
#' }
download_nasa_power_data <- function(df, lat_col = "latitude", lon_col = "longitude",
                                     start_date = "20150101", end_date = "20150305",
                                     parameters = c("T2M", "T2M_MAX", "T2M_MIN"),
                                     temporal_resolution = "daily", community = "RE",
                                     processes = 5, output_folder = "./nasa_power_data",
                                     start_col = NULL, end_col = NULL, id_col = NULL,
                                     return_consolidated = TRUE, overwrite = FALSE,
                                     verbose = TRUE) {
    if (inherits(df, "sf")) df <- sf::st_drop_geometry(df)
    df <- as.data.frame(df)
    if (!all(c(lat_col, lon_col) %in% names(df))) {
        stop("df must have the columns '", lat_col, "' and '", lon_col, "'", call. = FALSE)
    }
    build_url(0, 0, parameters, temporal_resolution, community, start_date, end_date)
    if (!is.null(id_col) && anyDuplicated(df[[id_col]])) {
        stop("'", id_col, "' has repeated values; each point needs its own identifier", call. = FALSE)
    }
    processes <- max(1L, min(as.integer(processes), 5L))
    dir.create(output_folder, showWarnings = FALSE, recursive = TRUE)

    fmt <- function(x) format(as.Date(as.character(x), tryFormats = c("%Y%m%d", "%Y-%m-%d")), "%Y%m%d")
    n <- nrow(df)
    pts <- data.frame(
        identifier = if (is.null(id_col)) paste0("idx_", seq_len(n)) else as.character(df[[id_col]]),
        latitude = df[[lat_col]], longitude = df[[lon_col]],
        start_date = if (is.null(start_col)) start_date else fmt(df[[start_col]]),
        end_date = if (is.null(end_col)) end_date else fmt(df[[end_col]]),
        stringsAsFactors = FALSE)
    if (temporal_resolution == "climatology") pts$start_date <- pts$end_date <- NA
    pts$filename <- paste0(pts$identifier, ".csv")
    pts$file <- file.path(output_folder, pts$filename)
    pts$url <- mapply(build_url, pts$latitude, pts$longitude,
                      MoreArgs = list(parameters = parameters,
                                      temporal_resolution = temporal_resolution,
                                      community = community),
                      start = pts$start_date, end = pts$end_date)
    if (verbose) {
        message(n, " points; parameters: ", paste(parameters, collapse = ", "), "; ",
                temporal_resolution, "; community ", community, "; ", processes, " processes")
    }
    t0 <- Sys.time()
    pts$success <- FALSE
    pts$error <- NA_character_
    todo <- if (overwrite) rep(TRUE, n) else
        !vapply(seq_len(n), function(i) .covers(pts[i, ], temporal_resolution, parameters), logical(1))
    pts$success[!todo] <- TRUE
    pts$error[!todo] <- "already downloaded"

    for (round in 0:3) {                 # first round and three retries
        idx <- which(todo & !pts$success)
        if (length(idx) == 0) break
        if (round > 0) {
            if (verbose) message("retrying ", length(idx), " failed points...")
            Sys.sleep(5 * 2^(round - 1))
        }
        res <- .fetch(pts$url[idx], processes)
        for (k in seq_along(idx)) {
            i <- idx[k]
            r <- res[[k]]
            if (!is.null(r$error)) {
                pts$error[i] <- r$error
                next
            }
            out <- tryCatch({
                d <- parse_response(r$body, temporal_resolution)
                d <- cbind(identifier = pts$identifier[i], latitude = pts$latitude[i],
                           longitude = pts$longitude[i], d)
                utils::write.csv(d, pts$file[i], row.names = FALSE)
                NULL
            }, error = function(e) conditionMessage(e))
            pts$success[i] <- is.null(out)
            pts$error[i] <- out %||% NA_character_
        }
        if (verbose) message("Downloaded: ", sum(pts$success), "/", n)
    }
    if (verbose) {
        message(sum(pts$success), "/", n, " points downloaded in ",
                round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1), " s")
        for (i in which(!pts$success)) message("  failed ", pts$identifier[i], ": ", pts$error[i])
    }
    pts$downloaded_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    res <- pts[, c("identifier", "latitude", "longitude", "start_date", "end_date",
                   "filename", "success", "error", "downloaded_at")]
    if (!return_consolidated || !any(pts$success)) return(res)
    data <- .consolidate(pts[pts$success, ], temporal_resolution)
    if (verbose) .report(data, pts[pts$success, ], temporal_resolution, parameters)
    data
}

# Download the URLs, up to `processes` at a time; busy answers (429, 5xx)
# and network errors come back as errors, to be retried.
.fetch <- function(urls, processes) {
    out <- vector("list", length(urls))
    pool <- curl::new_pool(total_con = processes, host_con = processes)
    for (k in seq_along(urls)) {
        local({
            kk <- k
            h <- curl::new_handle(timeout = 120)
            curl::multi_add(h, url = urls[kk], pool = pool,
                done = function(r) {
                    out[[kk]] <<- if (r$status_code == 429 || r$status_code >= 500) {
                        list(error = paste("HTTP", r$status_code))
                    } else {
                        list(body = rawToChar(r$content))
                    }
                },
                fail = function(msg) out[[kk]] <<- list(error = msg))
        })
    }
    curl::multi_run(pool = pool)
    out
}

.keys <- function(temporal_resolution) {
    switch(temporal_resolution, daily = "date", monthly = c("year", "month"), "period")
}

.in_period <- function(d, start, end, temporal_resolution) {
    if (temporal_resolution == "daily") {
        x <- as.Date(d$date)
        d[x >= as.Date(start, "%Y%m%d") & x <= as.Date(end, "%Y%m%d"), , drop = FALSE]
    } else if (temporal_resolution == "monthly") {
        d[d$year >= as.integer(substr(start, 1, 4)) & d$year <= as.integer(substr(end, 1, 4)), , drop = FALSE]
    } else {
        d
    }
}

.read_point <- function(file) {
    d <- utils::read.csv(file, colClasses = c(identifier = "character"), check.names = FALSE)
    if ("date" %in% names(d)) d$date <- as.Date(d$date)
    d
}

# TRUE if the CSV of a point already has all the period, with data in every row
.covers <- function(p, temporal_resolution, parameters) {
    if (!file.exists(p$file)) return(FALSE)
    tryCatch({
        d <- .read_point(p$file)
        if (!all(parameters %in% names(d))) return(FALSE)
        if (temporal_resolution == "daily") {
            if (min(d$date) > as.Date(p$start_date, "%Y%m%d") ||
                max(d$date) < as.Date(p$end_date, "%Y%m%d")) return(FALSE)
        } else if (temporal_resolution == "monthly") {
            if (min(d$year) > as.integer(substr(p$start_date, 1, 4)) ||
                max(d$year) < as.integer(substr(p$end_date, 1, 4))) return(FALSE)
        }
        d <- .in_period(d, p$start_date, p$end_date, temporal_resolution)
        !any(rowSums(!is.na(d[, parameters, drop = FALSE])) == 0)
    }, error = function(e) FALSE)
}

.consolidate <- function(pts, temporal_resolution) {
    parts <- lapply(seq_len(nrow(pts)), function(i) {
        .in_period(.read_point(pts$file[i]), pts$start_date[i], pts$end_date[i], temporal_resolution)
    })
    data <- do.call(rbind, parts)
    keys <- c("identifier", .keys(temporal_resolution))
    data <- data[!duplicated(data[, keys], fromLast = TRUE), , drop = FALSE]
    data <- data[do.call(order, unname(as.list(data[, keys, drop = FALSE]))), , drop = FALSE]
    rownames(data) <- NULL
    data
}

.report <- function(data, pts, temporal_resolution, parameters) {
    message(format(nrow(data), big.mark = ","), " rows; ", length(unique(data$identifier)),
            " points; no duplicates")
    expected <- switch(temporal_resolution,
        daily = as.numeric(as.Date(pts$end_date, "%Y%m%d") - as.Date(pts$start_date, "%Y%m%d")) + 1,
        monthly = (as.integer(substr(pts$end_date, 1, 4)) - as.integer(substr(pts$start_date, 1, 4)) + 1) * 12,
        NULL)
    if (!is.null(expected)) {
        rows <- table(factor(data$identifier, levels = pts$identifier))
        message("complete: ", sum(as.vector(rows) >= expected), "/", nrow(pts),
                " points with all the period")
    }
    empty <- rowSums(!is.na(data[, parameters, drop = FALSE])) == 0
    if (any(empty)) {
        k <- data[empty, .keys(temporal_resolution)[1]]
        message(sum(empty), " rows without data (NA), from ", min(k), " to ", max(k),
                " - days not yet published by NASA POWER, they are asked again in the next run")
    }
}
