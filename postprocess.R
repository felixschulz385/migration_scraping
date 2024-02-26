# Load necessary libraries
library(tidyverse)  # For data manipulation and visualization
library(future)     # For parallel processing
library(furrr)      # For parallel mapping functions
library(arrow)      # For reading and writing Parquet files
library(sf)         # For working with spatial data

# Example: Read a Parquet file and write to a CSV file
example_series <- read_parquet("/pfs/work7/workspace/scratch/tu_zxobe27-baten_migration/data/AH1.SG.parquet")
example_series %>% write_excel_csv("example_series.csv")

# Read all Parquet files in a directory, process them, and save the result to a CSV file

# List all Parquet files in the directory
all_files <- list.files("/pfs/work7/workspace/scratch/tu_zxobe27-baten_migration/data/", 
                        pattern = "parquet", full.names = TRUE)

# Define a function to process each Parquet file
parser <- function(file) {
  read_parquet(file) %>%
    group_by(search_id, field_z) %>%
    slice_max(order_by = probability, n = 1) %>%
    ungroup() %>%
    select(id=search_id, search_time, probability, field_x, field_y)
}

# Configure parallel processing
plan(multicore(workers = 4))

# Process all Parquet files in parallel and combine the results into a single dataframe
all_data <- future_map_dfr(all_files, parser)

# Write the combined data to a CSV file
all_data %>% write_excel_csv("aDNA_decennial_maximum.csv")

# Read the processed CSV file
all_data <- read_csv("aDNA_decennial_maximum.csv")

# Convert x and y coordinates to spatial points and project to a different coordinate reference system (CRS)

# Create spatial points from x and y coordinates
geometries <- st_sfc(map2(all_data$field_x, all_data$field_y, ~st_point(c(.x, .y))))

# Set the geometry column of the dataframe
all_data_projected <- st_set_geometry(all_data, geometries)

# Set the CRS of the dataframe
all_data_projected <- st_set_crs(all_data_projected, 3035)
all_data_projected <- st_transform(all_data_projected, 4326)

# Extract latitude and longitude from the transformed coordinates
all_data_projected$field_lon <- st_coordinates(all_data_projected)[,"X"]
all_data_projected$field_lat <- st_coordinates(all_data_projected)[,"Y"]

# Drop the geometry column
all_data_projected <- st_drop_geometry(all_data_projected)

# Remove original x and y columns
all_data_projected <- all_data_projected %>% select(-field_x, -field_y)

# Write the projected data to a CSV file
all_data_projected %>% write_excel_csv("aDNA_decennial_maximum_projected.csv")

# Zip the CSV file
zip("aDNA_decennial_maximum_projected.zip", "aDNA_decennial_maximum_projected.csv",
    zip = "/usr/bin/zip")
