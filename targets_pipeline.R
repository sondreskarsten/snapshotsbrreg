
library(targets)
library(httr)
library(googledrive)

tar_option_set(packages = c("httr", "googledrive"))

download_brreg_data <- function() {
  enhets_url <- "https://data.brreg.no/enhetsregisteret/api/enheter/lastned/csv"
  underenhets_url <- "https://data.brreg.no/enhetsregisteret/api/underenheter/lastned/csv"
  
  enhets_file <- tempfile(fileext = ".csv")
  underenhets_file <- tempfile(fileext = ".csv")
  
  GET(enhets_url, write_disk(enhets_file, overwrite = TRUE))
  GET(underenhets_url, write_disk(underenhets_file, overwrite = TRUE))
  
  list(enhets_file = enhets_file, underenhets_file = underenhets_file)
}

upload_to_drive <- function(files, folder_name = "Brreg_Data") {
  drive_auth()
  
  folder <- drive_get(folder_name)
  if (nrow(folder) == 0) {
    folder <- drive_mkdir(folder_name)
  }
  
  date_suffix <- format(Sys.Date(), "%Y-%m-%d")
  
  enhets_drive_name <- paste0("EnhetsRegisteret_", date_suffix, ".csv")
  underenhets_drive_name <- paste0("UnderEnhetsRegisteret_", date_suffix, ".csv")
  
  drive_upload(media = files$enhets_file, path = folder, name = enhets_drive_name, overwrite = TRUE)
  drive_upload(media = files$underenhets_file, path = folder, name = underenhets_drive_name, overwrite = TRUE)
}

list(
  tar_target(brreg_files, download_brreg_data(), format = "file"),
  tar_target(upload_result, upload_to_drive(brreg_files))
)

 