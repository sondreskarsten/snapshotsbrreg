
library(targets)
library(httr)
library(googledrive)
library(arrow)
library(future) 

plan(multisession, workers = 2)

tar_option_set(packages = c("httr", "googledrive", "arrow"), deployment = "worker")

download_file <- function(url) {
  file <- tempfile(fileext = ".csv")
  GET(url, write_disk(file, overwrite = TRUE))
  file
}

csv_to_parquet <- function(csv_file, type, base_path = "data") {
  date <- format(Sys.Date(), "%Y-%m-%d")
  parquet_path <- file.path(base_path, paste0(type, "_", date, ".parquet"))
  dir.create(dirname(parquet_path), recursive = TRUE, showWarnings = FALSE)
  
  df <- read.csv(csv_file)
  write_parquet(df, parquet_path)
  parquet_path
}

upload_to_drive <- function(file, folder_name = "Brreg_Data") {
  drive_auth()

  folder <- drive_get(folder_name)
  if (nrow(folder) == 0) {
    folder <- drive_mkdir(folder_name)
  }

  drive_upload(file, path = folder, name = basename(file), overwrite = TRUE)
}

list(
  tar_target(enhets_csv, download_file("https://data.brreg.no/enhetsregisteret/api/enheter/lastned/csv"), format = "file"),
  tar_target(underenhets_csv, download_file("https://data.brreg.no/enhetsregisteret/api/underenheter/lastned/csv"), format = "file"),

  tar_target(enhets_parquet, csv_to_parquet(enhets_csv, "EnhetsRegisteret"), format = "file"),
  tar_target(underenhets_parquet, csv_to_parquet(underenhets_csv, "UnderEnhetsRegisteret"), format = "file"),

  tar_target(upload_enhets_parquet, upload_to_drive(enhets_parquet)),
  tar_target(upload_underenhets_parquet, upload_to_drive(underenhets_parquet))
)

