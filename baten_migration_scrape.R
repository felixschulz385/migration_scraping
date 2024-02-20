#remotes::install_github('nevrome/mobest')

# read in command line arguments
args <- commandArgs(trailingOnly = TRUE)
#args <- c(1)

# Load necessary libraries
library("tidyverse")    # For data manipulation and visualization
library("arrow")        # For reading and writing data in different formats
library("mobest")       # For spatial and temporal modeling

###
# Parameters
###

# research area
research_area_4326 <- sf::st_polygon(
  list(
    cbind(
      c(35.91,11.73,-11.74,-15.47,37.06,49.26,49.56,35.91), # longitudes
      c(25.61,28.94, 31.77, 62.73,65.67,44.56,28.55,25.61)  # latitudes
    )
  )
) %>% sf::st_sfc(crs = 4326)

search_years <- seq(-2500, 0, by = 10)

###
# Execute search
###

# get the worldwide land outline
worldwide_land_outline_4326 <- rnaturalearth::ne_download(
  scale = 50, type = 'land', category = 'physical',
  returnclass = "sf"
)

# cut out research area
research_land_outline_4326 <- sf::st_intersection(
  worldwide_land_outline_4326,
  research_area_4326
)

# project outline to target CRS
research_land_outline_3035 <- research_land_outline_4326 %>% sf::st_transform(crs = 3035)

# create a projection grid
spatial_pred_grid <- mobest::create_prediction_grid(
  research_land_outline_3035,
  spatial_cell_size = 50000
)

# read in the samples
samples_basic <- readr::read_csv("samples_basic.csv")

# project the samples to the target CRS
samples_projected <- samples_basic %>%
  sf::st_as_sf(
    coords = c("Longitude", "Latitude"),
    crs = 4326
  ) %>%
  sf::st_transform(crs = 3035) %>%
  dplyr::mutate(
    x = sf::st_coordinates(.)[,1],
    y = sf::st_coordinates(.)[,2]
  ) %>%
  sf::st_drop_geometry()

# prepare input data for interpolation
ind <- mobest::create_spatpos(
  id = samples_projected$Sample_ID,
  x  = samples_projected$x,
  y  = samples_projected$y,
  z  = samples_projected$Date_BC_AD_Median
)
dep <- mobest::create_obs(
  C1 = samples_projected$MDS_C1,
  C2 = samples_projected$MDS_C2
)

kernset <- mobest::create_kernset(
  C1 = mobest::create_kernel(
    dsx = 800 * 1000, dsy = 800 * 1000, dt = 800,
    g = 0.1
  ),
  C2 = mobest::create_kernel(
    dsx = 800 * 1000, dsy = 800 * 1000, dt = 800,
    g = 0.1
  )
)

run_search <- function(index){

    search_samples <- samples_projected %>% slice(index)

    search_ind <- mobest::create_spatpos(
        id = search_samples$Sample_ID,
        x  = search_samples$x,
        y  = search_samples$y,
        z  = search_samples$Date_BC_AD_Median
    )
    search_dep <- mobest::create_obs(
        C1 = search_samples$MDS_C1,
        C2 = search_samples$MDS_C2
    )

    search_result <- mobest::locate(
        independent        = ind,
        dependent          = dep,
        kernel             = kernset,
        search_independent = search_ind,
        search_dependent   = search_dep,
        search_space_grid  = spatial_pred_grid,
        search_time        = search_years,
        search_time_mode   = "relative"
    )

    search_product <- mobest::multiply_dependent_probabilities(search_result)

    write_parquet(search_product, paste0("data/", samples_projected$Sample_ID[[index]], ".parquet"))

}

# # multicore plan
# cl = makeCluster(4)
# plan(cluster, workers = cl)

# execution
#start <- Sys.time()
run_search(as.double(args[[1]]))
#Sys.time() - start
