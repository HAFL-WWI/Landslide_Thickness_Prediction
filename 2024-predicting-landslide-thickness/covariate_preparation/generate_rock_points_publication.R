######################################################################
# Copyright (C) 2024 BFH
#
# Script for generating random points within the swissTLM3D rock signature.
# Points are generated only within catchments actually containing slides. 
# The number of generated points depends on the inventory size and the 
# percentage of the catchments covered by rock.
#
# Author: Christoph Schaller, BFH-HAFL, March 2024
######################################################################

#
# Initial configuration
#

gdal_base_path <- "C:/OSGeo4W/bin"
Sys.setenv(GDAL_DATA=gdal_base_path)
Sys.setenv(GDAL_DRIVER_PATH=gdal_base_path)
# Sys.setenv(PROJ_PATH=file.path(sf_path,"proj"))
Sys.getenv("GDAL_DATA")

library(sf)
library(terra)

library(dplyr)
library(parallel)

base_path <- "E:/GIS_Projekte/Paper_2"
setwd(base_path)

output_base_path <- "E:/GIS_Projekte/Paper_2/data/geomorph"

watershed_path <- "./data/EZG_Gewaesser_aggregiert_150.gpkg"
watershed_layer <- "EZG_Gewaesser_aggregiert_150"
watershed_mask_path <- "EZG_TEZGNR150_mask.tif"
municipality_path <- "E:/GIS_Projekte/Geodaten/SwissBounderies_2013/BOUNDARIES_2013/swissBOUNDARIES3D/SHAPEFILE_LV95_LN02/swissBOUNDARIES3D_1_1_TLM_HOHEITSGEBIET.shp"
dem_path <- "dhm_CH_5_filled.tif"
gc_path <- "ground_cover_tlm_5m.tif"

slide_path <-"../hangmuren-export-2023-04-10-10-06.csv"
storme_slide_path <-"../storme_interlis_merged_selection.csv"

setwd(output_base_path)

tile_id_name <- "TEZGNR150"

#Generate common rasters

#output_base_path <- "./data/geomorph"
#setwd(output_base_path)

watershed_mask <- rast(watershed_mask_path)
gc <- rast(gc_path)
dem <- rast(dem_path)

rock_mask_strict_path <- "rock_mask_strict_5m.tif"

rock_mask_strict<-NA

if(!file.exists(rock_mask_strict_path)) {
  #   rock_mask_iberal <- terra::app(gc, function(x) x %in% c(1,2,3,4))
  # rock_mask_strict<- terra::app(gc, fun=function(x) x %in% c(1,2))
  rock_mask_strict <- (gc==1)+(gc==2)
  
  writeRaster(rock_mask_strict, rock_mask_strict_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  rock_mask_strict <- rast(rock_mask_strict_path)
}


slope_mask_liberal_path <- "slope_mask_liberal_5m.tif"
slope_mask_strict_path <- "slope_mask_strict_5m.tif"
slope_mask_lt15_path <- "slope_mask_lt15_5m.tif"
slope_mask_liberal <- NA
slope_mask_strict <- NA
slope_mask_lt15 <- NA


if(!file.exists(slope_mask_liberal_path)) {
  slope <- terrain(dem, v="slope", neighbors=8, unit="degrees") 
  slope_mask_liberal <- slope>45 
  writeRaster(slope_mask_liberal, slope_mask_liberal_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  
} else {
  slope_mask_liberal <- rast(slope_mask_liberal_path)
}

if(!file.exists(slope_mask_strict_path)) {
  slope <- terrain(dem, v="slope", neighbors=8, unit="degrees") 
  slope_mask_strict <- slope>50
  writeRaster(slope_mask_strict, slope_mask_strict_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  
} else {
  slope_mask_strict <- rast(slope_mask_strict_path)
}

if(!file.exists(slope_mask_lt15_path)) {
  slope <- terrain(dem, v="slope", neighbors=8, unit="degrees") 
  slope_mask_lt15 <- slope<15
  writeRaster(slope_mask_lt15, slope_mask_lt15_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  
} else {
  slope_mask_lt15 <- rast(slope_mask_lt15_path)
}

# Function for randomly sampling points in a raster mask with
sampleRockPoints <- function(raster_mask, n_points, method='random', exhaustive = TRUE) {
  raster_mask[raster_mask==0] <- NA
  
  pts_rast <- spatSample(raster_mask, size = n_points, method = method, na.rm = TRUE, xy=TRUE, exhaustive=exhaustive)
  nrow(pts_rast)
  head(pts_rast)
  
  pts_rast.spdf <- st_as_sf(pts_rast, coords = c("x", "y"), crs = crs.lv95)
  
  pts_rast.spdf$x <- pts_rast$x
  pts_rast.spdf$Y <- pts_rast$y
  
  municipalities <- st_read(municipality_path)
  
  nrow(pts_rast.spdf)
  pts_rast.joined <- st_join(x = pts_rast.spdf, y = municipalities)
  nrow(pts_rast.joined)
  
  pts_rast$gemeinde <- pts_rast.joined$NAME
  
  return(pts_rast)
  
}



#
# Generate points for HMDB data
#
base_path <- "E:/GIS_Projekte/Paper_2"
setwd(base_path)

slide_path <-"./data/hangmuren-export-2023-04-10-10-06.csv"

hm.df <-  read.csv(slide_path, stringsAsFactors = FALSE, sep = ";", quote = '"', encoding="UTF-8", skip = 0)
head(hm.df)
colnames(hm.df)
str(hm.df)

crs.lv95 <- st_crs("EPSG:2056")

hm.spdf <- st_as_sf(hm.df, coords = c("X.Koordinate", "Y.Koordinate"), crs = crs.lv95)

hm.spdf$X.Koordinate <- hm.df$X.Koordinate
hm.spdf$Y.Koordinate <- hm.df$Y.Koordinate

watersheds <- st_read(watershed_path, layer=watershed_layer)

st_crs(hm.spdf)
st_crs(watersheds)

hm.joined <- st_join(x = hm.spdf, y = watersheds)
nrow(hm.joined)
hm.joined <- hm.joined[!is.na(hm.joined$TEZGNR150),]
nrow(hm.joined)

watersheds.target <- unique(hm.joined$TEZGNR150[!is.na(hm.joined$TEZGNR150)])
watersheds.target

#Paths and configs
output_base_path <- "./data/geomorph"

setwd(output_base_path)

watersheds.target.num <- sapply(watersheds.target,function(x) as.numeric(x))

area_mask_hmdb_path <- "area_mask_hmdb.tif"
area_mask_hmdb <- NA
if(!file.exists(area_mask_hmdb_path)) {
  area_mask_hmdb <- terra::app(watershed_mask, function(x) x %in% watersheds.target.num)
  writeRaster(area_mask_hmdb, area_mask_hmdb_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  
} else {
  area_mask_hmdb <- rast(area_mask_hmdb_path)
}

target_area_mask_hmdb_liberal_path <- "target_area_mask_hmdb_slope_unrestricted.tif"
target_area_mask_hmdb_strict_path <- "target_area_mask_hmdb_slope_restricted.tif"
target_area_mask_hmdb_lt15_path <- "target_area_mask_hmdb_slope_lt15.tif"
target_area_mask_hmdb_liberal <- NA
target_area_mask_hmdb_strict <- NA
target_area_mask_hmdb_lt15 <- NA


if(!file.exists(target_area_mask_hmdb_liberal_path)) {
  target_area_mask_hmdb_liberal <- area_mask_hmdb & rock_mask_strict
  writeRaster(target_area_mask_hmdb_liberal, target_area_mask_hmdb_liberal_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  target_area_mask_hmdb_liberal <- rast(target_area_mask_hmdb_liberal_path)
}

if(!file.exists(target_area_mask_hmdb_strict_path)) {
  target_area_mask_hmdb_strict <- area_mask_hmdb & rock_mask_strict & slope_mask_liberal
  writeRaster(target_area_mask_hmdb_strict, target_area_mask_hmdb_strict_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  target_area_mask_hmdb_strict <- rast(target_area_mask_hmdb_strict_path)
}

if(!file.exists(target_area_mask_hmdb_lt15_path)) {
  target_area_mask_hmdb_lt15 <- area_mask_hmdb & slope_mask_lt15
  writeRaster(target_area_mask_hmdb_lt15, target_area_mask_hmdb_lt15_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  target_area_mask_hmdb_lt15 <- rast(target_area_mask_hmdb_lt15_path)
}

area_hmdb_sum <- terra::global(area_mask_hmdb, fun="sum", na.rm=TRUE)
target_hmdb_liberal_sum <- terra::global(target_area_mask_hmdb_liberal, fun="sum", na.rm=TRUE)
target_hmdb_strict_sum <- terra::global(target_area_mask_hmdb_strict, fun="sum", na.rm=TRUE)

hmdb_unrestricted_sample_rate <- target_hmdb_liberal_sum$sum / area_hmdb_sum$sum
hmdb_restricted_sample_rate <- target_hmdb_strict_sum$sum / area_hmdb_sum$sum
hmdb_unrestricted_sample_rate
hmdb_restricted_sample_rate
hmdb_unrestricted_sample_count <- ceiling(hmdb_unrestricted_sample_rate * nrow(hm.df)) + 10
hmdb_restricted_sample_count <- ceiling(hmdb_restricted_sample_rate * nrow(hm.df)) + 10
hmdb_unrestricted_sample_count
hmdb_restricted_sample_count

sample_points_liberal <- sampleRockPoints(target_area_mask_hmdb_liberal,nrow(hm.df))
slide_out_path <-"./../hmdb_sampled_slope_unrestricted.csv"
write.csv(sample_points_liberal,slide_out_path, row.names = FALSE)

sample_points_strict <- sampleRockPoints(target_area_mask_hmdb_strict,nrow(hm.df))
slide_out_path <-"./../hmdb_sampled_slope_restricted.csv"
write.csv(sample_points_strict,slide_out_path, row.names = FALSE)

sample_points_lt15 <- sampleRockPoints(target_area_mask_hmdb_lt15,nrow(hm.df))
slide_out_path <-"./../hmdb_sampled_slope_lt15.csv"
write.csv(sample_points_lt15,slide_out_path, row.names = FALSE)

sample_points_liberal_uniform <- sampleRockPoints(target_area_mask_hmdb_liberal,hmdb_unrestricted_sample_count,method = 'random')
slide_out_path <-"./../hmdb_sampled_slope_unrestricted_proportional.csv"
write.csv(sample_points_liberal_uniform,slide_out_path, row.names = FALSE)

sample_points_strict_uniform <- sampleRockPoints(target_area_mask_hmdb_strict,hmdb_restricted_sample_count,method = 'random')
slide_out_path <-"./../hmdb_sampled_slope_restricted_proportional.csv"
write.csv(sample_points_strict_uniform,slide_out_path, row.names = FALSE)






#
# Generate points for KtBE data
#


base_path <- "E:/GIS_Projekte/Paper_2"
setwd(base_path)

slide_path <- "./data/SpontRutschBE.gpkg"

hm.df <- st_read(slide_path)
head(hm.df)
colnames(hm.df)
str(hm.df)

st_crs(hm.df)
crs.lv95 <- st_crs("EPSG:2056")

hm.spdf <- hm.df

hm.spdf$X.Koordinate <- hm.df$x
hm.spdf$Y.Koordinate <- hm.df$y

st_layers(watershed_path)

watersheds <- st_read(watershed_path, layer=watershed_layer)

st_crs(hm.spdf)
st_crs(watersheds)

hm.joined <- st_join(x = hm.spdf, y = watersheds)
nrow(hm.joined)
hm.joined$TEZGNR150 <- as.character(hm.joined$TEZGNR150)
hm.joined$TEZGNR150[hm.joined$bi_BI_DATENHERR=="FL"] <- "liLI"
hm.joined <- hm.joined[!is.na(hm.joined$TEZGNR150),]
nrow(hm.joined)

watersheds.target <- unique(hm.joined$TEZGNR150[!is.na(hm.joined$TEZGNR150)])
watersheds.target

output_base_path <- "./data/geomorph"
setwd(output_base_path)


watersheds.target.num <- sapply(watersheds.target,function(x) as.numeric(x))

area_mask_be_path <- "area_mask_be.tif"
area_mask_be <- NA
if(!file.exists(area_mask_be_path)) {
  area_mask_be <- terra::app(watershed_mask, function(x) x %in% watersheds.target.num)
  writeRaster(area_mask_be, area_mask_be_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  
} else {
  area_mask_be <- rast(area_mask_be_path)
}

target_area_mask_be_liberal_path <- "target_area_mask_be_slope_unrestricted.tif"
target_area_mask_be_strict_path <- "target_area_mask_be_slope_restricted.tif"
target_area_mask_be_lt15_path <- "target_area_mask_be_slope_lt15.tif"

target_area_mask_be_liberal <- NA
target_area_mask_be_strict <- NA
target_area_mask_be_lt15 <- NA

if(!file.exists(target_area_mask_be_liberal_path)) {
  target_area_mask_be_liberal <- area_mask_be & rock_mask_strict 
  writeRaster(target_area_mask_be_liberal, target_area_mask_be_liberal_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  target_area_mask_be_liberal <- rast(target_area_mask_be_liberal_path)
}

if(!file.exists(target_area_mask_be_strict_path)) {
  target_area_mask_be_strict <- area_mask_be & rock_mask_strict & slope_mask_liberal
  writeRaster(target_area_mask_be_strict, target_area_mask_be_strict_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  target_area_mask_be_strict <- rast(target_area_mask_be_strict_path)
}

if(!file.exists(target_area_mask_be_lt15_path)) {
  target_area_mask_be_lt15 <- area_mask_be & slope_mask_lt15
  writeRaster(target_area_mask_be_lt15, target_area_mask_be_lt15_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  target_area_mask_be_lt15 <- rast(target_area_mask_be_lt15_path)
}

area_be_sum <- terra::global(area_mask_be, fun="sum", na.rm=TRUE)
target_be_liberal_sum <- terra::global(target_area_mask_be_liberal, fun="sum", na.rm=TRUE)
target_be_strict_sum <- terra::global(target_area_mask_be_strict, fun="sum", na.rm=TRUE)

be_unrestricted_sample_rate <- target_be_liberal_sum$sum / area_be_sum$sum
be_restricted_sample_rate <- target_be_strict_sum$sum / area_be_sum$sum
be_unrestricted_sample_rate
be_restricted_sample_rate
be_unrestricted_sample_count <- ceiling(be_unrestricted_sample_rate * nrow(hm.df)) + 10
be_restricted_sample_rate_count <- ceiling(be_restricted_sample_rate * nrow(hm.df)) + 10
  

sample_points_liberal <- sampleRockPoints(target_area_mask_be_liberal,nrow(hm.df))
slide_out_path <-"./../be_sampled_slope_unrestricted.csv"
write.csv(sample_points_liberal,slide_out_path, row.names = FALSE)

sample_points_strict <- sampleRockPoints(target_area_mask_be_strict,nrow(hm.df))
slide_out_path <-"./../be_sampled_slope_restricted.csv"
write.csv(sample_points_strict,slide_out_path, row.names = FALSE)

sample_points_lt15 <- sampleRockPoints(target_area_mask_be_lt15,nrow(hm.df))
slide_out_path <-"./../be_sampled_slope_lt15.csv"
write.csv(sample_points_lt15,slide_out_path, row.names = FALSE)


sample_points_liberal <- sampleRockPoints(target_area_mask_be_liberal,be_unrestricted_sample_count,method='random')
slide_out_path <-"./../be_sampled_slope_unrestricted_proportional.csv"
write.csv(sample_points_liberal,slide_out_path, row.names = FALSE)

sample_points_strict <- sampleRockPoints(target_area_mask_be_strict,be_restricted_sample_rate_count,method='random')
slide_out_path <-"./../be_sampled_slope_restricted_proportional.csv"
write.csv(sample_points_strict,slide_out_path, row.names = FALSE)


#
# Generate points for StorMe data
#
base_path <- "E:/GIS_Projekte/Paper_2"
setwd(base_path)

slide_path <-"./data/storme_interlis_merged_selection.csv"
getwd()

hm.df <-  read.csv(slide_path, stringsAsFactors = FALSE, sep = ";", quote = '"', encoding="UTF-8", skip = 0)
head(hm.df)
colnames(hm.df)
str(hm.df)


crs.lv95 <- st_crs("EPSG:2056")


hm.df$X.Koordinate <- hm.df$x
hm.df$Y.Koordinate <- hm.df$y

hm.spdf <- st_as_sf(hm.df, coords = c("x", "y"), crs = crs.lv95)

hm.spdf$X.Koordinate <- hm.df$X.Koordinate
hm.spdf$Y.Koordinate <- hm.df$Y.Koordinate

watersheds <- st_read(watershed_path, layer=watershed_layer)

hm.joined <- st_join(x = hm.spdf, y = watersheds)
nrow(hm.joined)
hm.joined$TEZGNR150 <- as.character(hm.joined$TEZGNR150)
#hm.joined$TEZGNR150[hm.joined$bi_BI_DATENHERR=="FL"] <- "liLI"
hm.joined <- hm.joined[!is.na(hm.joined$TEZGNR150),]
nrow(hm.joined)

#hm.joined <- hm.joined[(!is.na(hm.joined$di_r_DI_R_ANRISS_HOEHE_MAO) & hm.joined$di_r_DI_R_ANRISS_HOEHE_MAO=="Messwert_Feststellung"),]
hm.joined <- hm.joined[(!is.na(hm.joined$di_r_DI_R_ANRISS_HOEHE) ),]

watersheds.target <- unique(hm.joined$TEZGNR150[!is.na(hm.joined$TEZGNR150)])
watersheds.target

#Paths and configs
output_base_path <- "./data/geomorph"

setwd(output_base_path)

watersheds.target.num <- sapply(watersheds.target,function(x) as.numeric(x))
area_mask_storme_path <- "area_mask_storme.tif"
area_mask_storme <- NA
if(!file.exists(area_mask_storme_path)) {
  area_mask_storme <- terra::app(watershed_mask, function(x) x %in% watersheds.target.num)
  writeRaster(area_mask_storme, area_mask_storme_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  
} else {
  area_mask_storme <- rast(area_mask_storme_path)
}

target_area_mask_storme_liberal_path <- "target_area_mask_storme_slope_unrestricted.tif"
target_area_mask_storme_strict_path <- "target_area_mask_storme_slope_restricted.tif"
target_area_mask_storme_lt15_path <- "target_area_mask_storme_slope_lt15.tif"
target_area_mask_storme_liberal <- NA
target_area_mask_storme_strict <- NA
target_area_mask_storme_lt15 <- NA

if(!file.exists(target_area_mask_storme_liberal_path)) {
  target_area_mask_storme_liberal <- area_mask_storme & rock_mask_strict
  writeRaster(target_area_mask_storme_liberal, target_area_mask_storme_liberal_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  target_area_mask_storme_liberal <- rast(target_area_mask_storme_liberal_path)
}

if(!file.exists(target_area_mask_storme_strict_path)) {
  target_area_mask_storme_strict <- area_mask_storme & rock_mask_strict & slope_mask_liberal
  writeRaster(target_area_mask_storme_strict, target_area_mask_storme_strict_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  target_area_mask_storme_strict <- rast(target_area_mask_storme_strict_path)
}

if(!file.exists(target_area_mask_storme_lt15_path)) {
  target_area_mask_storme_lt15 <- area_mask_storme & slope_mask_lt15
  writeRaster(target_area_mask_storme_lt15, target_area_mask_storme_lt15_path, filetype = "GTiff", datatype="INT2S", overwrite=TRUE,
              gdal=c("TILED=YES","COMPRESS=DEFLATE"))
} else {
  target_area_mask_storme_lt15 <- rast(target_area_mask_storme_lt15_path)
}

area_storme_sum <- terra::global(area_mask_storme, fun="sum", na.rm=TRUE)
target_storme_liberal_sum <- terra::global(target_area_mask_storme_liberal, fun="sum", na.rm=TRUE)
target_storme_strict_sum <- terra::global(target_area_mask_storme_strict, fun="sum", na.rm=TRUE)

storme_unrestricted_sample_rate <- target_storme_liberal_sum$sum / area_storme_sum$sum
storme_restricted_sample_rate <- target_storme_strict_sum$sum / area_storme_sum$sum
storme_unrestricted_sample_rate
storme_restricted_sample_rate
storme_unrestricted_sample_rate_count <- ceiling(storme_unrestricted_sample_rate * 2988) + 10
storme_restricted_sample_rate_count <- ceiling(storme_restricted_sample_rate * 2988) + 10

sample_points_liberal <- sampleRockPoints(target_area_mask_storme_liberal,2988)
slide_out_path <-"./../storme_sampled_slope_unrestricted.csv"
write.csv(sample_points_liberal,slide_out_path, row.names = FALSE)

sample_points_strict <- sampleRockPoints(target_area_mask_storme_strict,2988)
slide_out_path <-"./../storme_sampled_slope_restricted.csv"
write.csv(sample_points_strict,slide_out_path, row.names = FALSE)

sample_points_lt15 <- sampleRockPoints(target_area_mask_storme_lt15,2988)
slide_out_path <-"./../storme_sampled_slope_lt15.csv"
write.csv(sample_points_lt15,slide_out_path, row.names = FALSE)

sample_points_liberal_uniform <- sampleRockPoints(target_area_mask_storme_liberal,storme_unrestricted_sample_rate_count,method = 'random')
slide_out_path <-"./../storme_sampled_slope_unrestricted_proportional.csv"
write.csv(sample_points_liberal_uniform,slide_out_path, row.names = FALSE)

sample_points_strict_uniform <- sampleRockPoints(target_area_mask_storme_strict,storme_restricted_sample_rate_count,method = 'random')
slide_out_path <-"./../storme_sampled_slope_restricted_proportional.csv"
write.csv(sample_points_strict_uniform,slide_out_path, row.names = FALSE)

