daily_js <- function(days = 1:2, missing = integer()) {
    t2m <- stats::setNames(as.list(20 + days), sprintf("202401%02d", days))
    t2m[sprintf("202401%02d", missing)] <- -999
    pr <- stats::setNames(as.list(rep(0.5, length(days))), sprintf("202401%02d", days))
    jsonlite::toJSON(list(properties = list(parameter = list(T2M = t2m, PRECTOTCORR = pr))),
                     auto_unbox = TRUE)
}
fake_fetch <- function(body, calls) {
    function(urls, processes) {
        calls$n <- calls$n + length(urls)
        lapply(urls, function(u) list(body = body))
    }
}

test_that("build_url", {
    u <- build_url(20.97, -89.62, c("T2M", "PRECTOTCORR"), "daily", "AG", "20240101", "20241231")
    expect_match(u, "^https://power.larc.nasa.gov/api/temporal/daily/point\\?")
    expect_match(u, "parameters=T2M,PRECTOTCORR")
    expect_match(build_url(0, 0, "T2M", "monthly", start = "20240101", end = "20251231"),
                 "start=2024&end=2025")
    expect_no_match(build_url(0, 0, "T2M", "climatology"), "start=")
    expect_error(build_url(0, 0, "T2M", "hourly"))
    expect_error(build_url(0, 0, character(), "daily"))
})

test_that("parse_response", {
    d <- parse_response(daily_js(1:2, missing = 2))
    expect_named(d, c("date", "T2M", "PRECTOTCORR"))
    expect_equal(sum(is.na(d$T2M)), 1)
    m <- parse_response('{"properties":{"parameter":{"T2M":{"202401":22,"202402":23,"202413":26}}}}',
                        "monthly")
    expect_equal(m$month, 1:2)
    expect_error(parse_response('{"messages":["bad request"]}'), "bad request")
})

test_that("download, resume, update and no duplicates", {
    tmp <- tempfile(); on.exit(unlink(tmp, recursive = TRUE))
    calls <- new.env(); calls$n <- 0
    local_mocked_bindings(.fetch = fake_fetch(daily_js(1:10), calls))
    pts <- data.frame(id = c("01001", "01002"), latitude = c(21.8, 22.1), longitude = c(-102.3, -102.1))
    kw <- list(df = pts, id_col = "id", parameters = c("T2M", "PRECTOTCORR"),
               output_folder = tmp, verbose = FALSE)
    out <- do.call(download_nasa_power_data, c(kw, start_date = "20240101", end_date = "20240102"))
    expect_equal(nrow(out), 4)
    expect_equal(sort(unique(out$identifier)), c("01001", "01002"))   # leading zeros kept
    expect_true(file.exists(file.path(tmp, "01001.csv")))
    n <- calls$n
    do.call(download_nasa_power_data, c(kw, start_date = "20240101", end_date = "20240102"))
    expect_equal(calls$n, n)                                           # resumed: nothing asked
    out <- do.call(download_nasa_power_data, c(kw, start_date = "20240101", end_date = "20240112"))
    expect_equal(calls$n, n + 2)                                       # update: asked again
    out <- do.call(download_nasa_power_data, c(kw, start_date = "20240103", end_date = "20240104"))
    expect_equal(as.integer(format(out$date, "%d")), c(3L, 4L, 3L, 4L))
    expect_false(anyDuplicated(out[, c("identifier", "date")]) > 0)
    expect_error(download_nasa_power_data(rbind(pts, pts), id_col = "id", verbose = FALSE), "repeated")
})

test_that("unpublished days are asked again", {
    tmp <- tempfile(); on.exit(unlink(tmp, recursive = TRUE))
    calls <- new.env(); calls$n <- 0
    local_mocked_bindings(.fetch = fake_fetch(daily_js(1:2, missing = 2), calls))
    pts <- data.frame(id = "a", latitude = 21.8, longitude = -102.3)
    for (k in 1:2) {
        download_nasa_power_data(pts, id_col = "id", parameters = "T2M", start_date = "20240101",
                                 end_date = "20240102", output_folder = tmp, verbose = FALSE)
    }
    expect_equal(calls$n, 2)
})

test_that("failed points are retried", {
    tmp <- tempfile(); on.exit(unlink(tmp, recursive = TRUE))
    answers <- new.env(); answers$k <- 0
    local_mocked_bindings(.fetch = function(urls, processes) {
        answers$k <- answers$k + 1
        if (answers$k == 1) lapply(urls, function(u) list(error = "HTTP 429"))
        else lapply(urls, function(u) list(body = daily_js(1:2)))
    })
    local_mocked_bindings(Sys.sleep = function(time) NULL, .package = "base")
    pts <- data.frame(id = "a", latitude = 21.8, longitude = -102.3)
    out <- download_nasa_power_data(pts, id_col = "id", parameters = "T2M", start_date = "20240101",
                                    end_date = "20240102", output_folder = tmp, verbose = FALSE)
    expect_equal(nrow(out), 2)
})

test_that("municipalities", {
    mx <- municipalities()
    expect_equal(nrow(mx), 2465)
    expect_false(anyDuplicated(mx$CVEGEO) > 0)
    expect_equal(nrow(municipalities("31")), 106)
    expect_equal(nrow(municipalities(31)), 106)
    expect_setequal(unique(municipalities(c("14", "31"))$CVE_ENT), c("14", "31"))
    c31 <- municipalities("31", point = "centroid")
    expect_equal(c31$latitude, c31$centroid_lat)
    expect_error(municipalities("33"))
})

test_that("centroids", {
    skip_if_not_installed("sf")
    p <- sf::st_sf(CVEGEO = "a", geometry = sf::st_sfc(sf::st_polygon(list(
        rbind(c(-90, 20), c(-89, 20), c(-89, 21), c(-90, 21), c(-90, 20)))), crs = 4326))
    c1 <- centroids(p, "CVEGEO")
    expect_named(c1, c("CVEGEO", "latitude", "longitude"))
    expect_true(c1$latitude > 20 && c1$latitude < 21)
})
