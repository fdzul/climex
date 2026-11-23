# climex

**English** \| [Español](#espa%C3%B1ol)

Climate data of Mexico from the [NASA
POWER](https://power.larc.nasa.gov) API: daily, monthly or
climatological temperature, precipitation, humidity, wind and radiation
for the 2,465 municipalities of Mexico or any set of points. It is part
of the [dengueverse](https://fdzul.github.io/dengueverse/); the Python
version is [climex-py](https://github.com/fdzul/climex-py).

## Installation

``` r

install.packages("climex", repos = c("https://fdzul.r-universe.dev", "https://cloud.r-project.org"))
```

## Usage

``` r

library(climex)

yuc <- municipalities("31")          # the 106 municipalities of Yucatán; municipalities() = all the country
clima <- download_nasa_power_data(
    yuc, id_col = "CVEGEO",
    parameters = c("T2M", "T2M_MAX", "T2M_MIN", "PRECTOTCORR", "RH2M"),
    start_date = "20240101", end_date = "20241231",
    temporal_resolution = "daily", community = "AG")
```

The result has one row per municipality and day (`identifier`,
`latitude`, `longitude`, `date` and one column per parameter), only for
the period requested and without duplicates. Each point is saved as a
CSV in `output_folder`, so a download that stops can be run again and
continues where it stopped; with a later `end_date` (a weekly update)
only the new period is asked again.

Documentation: <https://fdzul.github.io/climex/>

------------------------------------------------------------------------

## Español

Datos climáticos de México de la API de [NASA
POWER](https://power.larc.nasa.gov): temperatura, precipitación,
humedad, viento y radiación diarios, mensuales o climatológicos para los
2,465 municipios de México o cualquier conjunto de puntos. Es parte del
[dengueverse](https://fdzul.github.io/dengueverse/); la versión en
Python es [climex-py](https://github.com/fdzul/climex-py).

``` r

library(climex)
yuc <- municipalities("31")          # municipios de Yucatán; municipalities() = todo el país
clima <- download_nasa_power_data(yuc, id_col = "CVEGEO",
                                  parameters = c("T2M", "PRECTOTCORR"),
                                  start_date = "20240101", end_date = "20241231")
```

El resultado tiene una fila por municipio y día, solo del periodo pedido
y sin duplicados. Si la descarga se interrumpe, se vuelve a correr y
continúa donde se quedó.
