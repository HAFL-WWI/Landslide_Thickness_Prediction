######################################################################
# Copyright (C) 2024 BFH
#
# Script for generating the covariate rasters used as input for the 
# modelling and analysis steps. The rasters are generated in tiles 
# based on aggregated catchments of about 150 km2 area.
#
# Author: Christoph Schaller, BFH-HAFL, March 2024
######################################################################

#
# Initial configuration
#

#sf_path <- find.package("sf")
# Sys.setenv(GDAL_DATA=file.path(sf_path,"gdal"))
# Sys.setenv(GDAL_DRIVER_PATH=file.path(sf_path,"gdal"))
# Sys.setenv(PROJ_PATH=file.path(sf_path,"proj"))
# Sys.getenv("GDAL_DATA")

# if (!require("sf")) install.packages("sf")
# if (!require("gdalUtils")) {
#   install.packages("devtools")
#   devtools:::install_github("gearslaboratory/gdalUtils")
# }
# if (!require("terra")) install.packages("terra")
# if (!require("RSAGA")) install.packages("RSAGA")
# if (!require("dplyr")) install.packages("dplyr")

gdal_base_path <- "C:/OSGeo4W/bin"
Sys.setenv(GDAL_DATA=gdal_base_path)
Sys.setenv(GDAL_DRIVER_PATH=gdal_base_path)
Sys.getenv("GDAL_DATA")

library(sf)
library(gdalUtilities)
library(terra)

library(RSAGA)
library(dplyr)
library(parallel)

#Paths, names and other configs
eudem_path <- "E:/GIS_Projekte/Geodaten/EU-DEM_v1_1/eu_dem_v11_E40N20.TIF"
swissalti_base_path <- "E:/GIS_Projekte/Geodaten/swissALTI3D_CH"
output_base_path <- "E:/GIS_Projekte/Paper_2/data/geomorph"

watershed_path <- "../EZG_Gewaesser_aggregiert_150.gpkg"
watershed_layer <- "EZG_Gewaesser_aggregiert_150"
slide_path <-"../hangmuren-export-2023-04-10-10-06.csv"
storme_slide_path <-"../storme_interlis_merged_selection.csv"

dem_05_vrt_name <- "dem_0_5m.vrt"
dem_05_name <- "dem_0_5m.tif"
dem_5_name <- "dem_5m.tif"
dem_10_name <- "dem_10m.tif"
dem_25_name <- "dem_25m.tif"

dem_5_filled_name <- "dem_5m_filled.tif"
dem_10_filled_name <- "dem_10m_filled.tif"
dem_25_filled_name <- "dem_25m_filled.tif"

dem_5_filler_name <- "dem_5m_filler.tif"

#Rounded Swiss extent plus a buffer of 4000m
extent <- c(2481300,1071200,2838000,1300000)

res_l1 <- 5
res_l2 <- 10
res_l3 <- 25

scale0 <- 20
scale1 <- 50
scale2 <- 200

country <- "CH"
perimeter_buffer <- 4000

epsg_lv95 <- 2056
crs_lv95 <- paste0("EPSG:",epsg_lv95)

setwd(output_base_path)

#
# Merging, filling and aggregating input DEM
#

#Merge 0.5m resolution tiles, set dryrun=FALSE to actually execute (takes a looong time)
gdalbuildvrt(gdalfile=file.path(swissalti_base_path,"*.tif"),output.vrt=file.path(swissalti_base_path,dem_05_vrt_name),dryrun = TRUE)
gdal_translate(src_dataset=file.path(swissalti_base_path,dem_05_vrt_name),dst_dataset=file.path(swissalti_base_path,dem_05_name),of="GTiff",co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"),dryrun = TRUE )

#Aggregate 0.5m to 5m, 10m and 25m
gdalwarp(srcfile=file.path(swissalti_base_path,dem_05_name),dstfile=file.path(swissalti_base_path,dem_5_name),of="GTiff",tr=c(res_l1,res_l1),r="average",co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"),dryrun=TRUE)
gdalwarp(srcfile=file.path(swissalti_base_path,dem_05_name),dstfile=file.path(swissalti_base_path,dem_10_name),of="GTiff",tr=c(res_l2,res_l2),r="average",co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"),dryrun=TRUE)
gdalwarp(srcfile=file.path(swissalti_base_path,dem_05_name),dstfile=file.path(swissalti_base_path,dem_25_name),of="GTiff",tr=c(res_l3,res_l3),r="average",co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"),dryrun=TRUE)

#Generate fill raster
dem_rast_path <- file.path(swissalti_base_path,dem_5_name)
dem_rast_fill_path <- file.path(output_base_path,dem_5_filler_name)

gdalwarp(srcfile=eudem_path,dstfile=dem_rast_fill_path, t_srs=crs_lv95, overwrite=TRUE, r = "bilinear", tr =  c(res_l1,res_l1), te = extent)

#Reading input rasters
bbox <- st_bbox(c(xmin = extent[1], xmax = extent[3], ymax = extent[2], ymin = extent[4]), crs = st_crs(epsg_lv95))

xmin <- extent[1]
ymin <- extent[2]
xmax <- extent[3]
ymax <- extent[4]

extent_output <- ext(xmin,xmax,ymin,ymax)

#Crop original raster
dem_file_name <- paste("dem",country,sep="_")
dem_resampled_file_name_l1 <- paste(dem_file_name,"_",res_l1,"_resampled.tif",sep="")  
dem_resampled_file_name_l2 <- paste(dem_file_name,"_",res_l2,"_resampled.tif",sep="")  
dem_resampled_file_name_l3 <- paste(dem_file_name,"_",res_l3,"_resampled.tif",sep="")  

gdalwarp(srcfile=dem_rast_path,dstfile=file.path(getwd(),paste0(dem_file_name,"_raw.tif")), t_srs=crs_lv95, overwrite=TRUE, r = "near", tr =  c(res_l1,res_l1), te = extent)
dem <- rast(paste0(dem_file_name,"_raw.tif"))

dem_res <- res(dem)[1]

dem_fill_crop <- rast(dem_rast_fill_path) 

compareGeom(dem,dem_fill_crop,ext=TRUE,crs=TRUE,res=TRUE)

dem_filled <- cover(dem, dem_fill_crop, values=NA, filename=dem_resampled_file_name_l1, overwrite=TRUE)

gdalwarp(srcfile=dem_resampled_file_name_l1,dstfile=dem_resampled_file_name_l2, t_srs=crs_lv95, overwrite=TRUE, r = "average", tr =  c(res_l2,res_l2))
gdalwarp(srcfile=dem_resampled_file_name_l1,dstfile=dem_resampled_file_name_l3, t_srs=crs_lv95, overwrite=TRUE, r = "average", tr =  c(res_l3,res_l3))

#
# Determine watersheds with slides 
#

#Read Slides and Watersheds
hm.df <-  read.csv(slide_path, stringsAsFactors = FALSE, sep = ";", quote = '"', encoding="UTF-8", skip = 0)

head(hm.df)
colnames(hm.df)
str(hm.df)

crs.lv95 <- st_crs("EPSG:2056")

hm.spdf <- st_as_sf(hm.df, coords = c("X.Koordinate", "Y.Koordinate"), crs = crs.lv95)

watersheds <- st_read(watershed_path, layer=watershed_layer)

hm.joined <- st_join(x = hm.spdf, y = watersheds)

storme.df <-  read.csv(storme_slide_path, stringsAsFactors = FALSE, sep = ";", quote = '"', encoding="UTF-8", skip = 0)
head(storme.df)
colnames(storme.df)
str(storme.df)

storme.spdf <- st_as_sf(storme.df, coords = c("x", "y"), crs = crs.lv95)

storme.joined <- st_join(x = storme.spdf, y = watersheds)
nrow(storme.joined)

watersheds.target <- append(unique(hm.joined$TEZGNR150[!is.na(hm.joined$TEZGNR150)]),
                            unique(storme.joined$TEZGNR150[!is.na(storme.joined$TEZGNR150)]))
watersheds.target <- append(watersheds.target,
                            list(120916,120126,120453))
watersheds.target <- unique(watersheds.target)

watersheds.filtered <- filter(watersheds, TEZGNR150 %in% watersheds.target)
nrow(watersheds.filtered)

#
# Preparing base DEM tiles
#
setwd(output_base_path)

# Generating Inputs
raster_resolution <- res_l1 #Target resolution

#Reading input rasters

# Function returning the extend of a spatial object rounded to a specific resolution and modified by the specified buffer
get_rounded_extent <- function (sobj,resolution,buffer){
  extent_input <- terra::ext(sobj)
  
  xmin <- resolution*floor(xmin(extent_input)/resolution)-resolution-buffer
  ymin <- resolution*floor(ymin(extent_input)/resolution)-resolution-buffer
  xmax <- resolution*floor(xmax(extent_input)/resolution)+resolution+buffer
  ymax <- resolution*floor(ymax(extent_input)/resolution)+resolution+buffer
  
  # extent_output <- ext(xmin,xmax,ymin,ymax)
  extent_output <- terra::ext(c(xmin,xmax,ymin,ymax))
  
  return(extent_output)
}

extent_output <- terra::ext(c(extent[1],extent[3],extent[2],extent[4]))

#Generating Outputs

# specify SAGA environment
saga_path <- "C:/Program Files/SAGA-GIS/"
saga_modules_path <- "C:/Program Files/SAGA-GIS/tools"
env <- rsaga.env(path=saga_path,modules=saga_modules_path)

#Load DEM
r.tmp <- rast(dem_resampled_file_name_l1)
dem_crs <- crs(r.tmp)

# Reading input DEM
r.dem_l1 <- rast(dem_resampled_file_name_l1) 
r.dem_l2 <- rast(dem_resampled_file_name_l2)
r.dem_l3 <- rast(dem_resampled_file_name_l3)

# write SAGA format 
dem_resampled_file_name_sdat_l1 <-paste(dem_file_name,"_",res_l1,"m_resampled.sdat",sep="")
dem_resampled_file_name_sdat_l2 <-paste(dem_file_name,"_",res_l2,"m_resampled.sdat",sep="")
dem_resampled_file_name_sdat_l3 <-paste(dem_file_name,"_",res_l3,"m_resampled.sdat",sep="")

dem_resampled_file_name_sgrd_l1 <-gsub(".sdat",".sgrd",dem_resampled_file_name_sdat_l1)
dem_resampled_file_name_sgrd_l2 <-gsub(".sdat",".sgrd",dem_resampled_file_name_sdat_l2)
dem_resampled_file_name_sgrd_l3 <-gsub(".sdat",".sgrd",dem_resampled_file_name_sdat_l3)

writeRaster(r.dem_l1, filename = dem_resampled_file_name_sdat_l1, filetype = "SAGA",
            overwrite = TRUE, NAflag = NAflag(r.dem_l1))
writeRaster(r.dem_l2, filename = dem_resampled_file_name_sdat_l2, filetype = "SAGA",
            overwrite = TRUE, NAflag = NAflag(r.dem_l2))
writeRaster(r.dem_l3, filename = dem_resampled_file_name_sdat_l3, filetype = "SAGA",
            overwrite = TRUE, NAflag = NAflag(r.dem_l3))

# Preparing for parallel processing
output_base_path <- "."
n.cores<-20

tile_folder <- "ch_tiles"
tile_name_template <- paste0("dem_tile_",res_l1,"_ezg")
tile_name_template_l2 <- paste0("dem_tile_",res_l2,"_ezg")
tile_name_template_l3 <- paste0("dem_tile_",res_l3,"_ezg")
tile_id_name <- "TEZGNR150"
tile_data <- watersheds.filtered

tile_folder_path <- file.path(output_base_path,tile_folder) 
dir.create(tile_folder_path,showWarnings =FALSE)

#
# General processing functions
#

# General function for processing the individual tiles in a parallel fashion based on a specified processing function
process_dem_tiled <- function(dem, tile_data, tile_folder, tile_name_template, tile_id_name, proc_func, tile_buffer = 500, tile_output_buffer = 50, n.cores = 25, fargs = NULL) {
  dem_res <- terra::res(rast(dem))[1]
  
  
  cl <- makeCluster(n.cores)
  
  clusterEvalQ(cl, {
    library(sf)
    library(gdalUtilities)
    library(terra)
    library(RSAGA)
    library(dplyr)
    library(parallel)
  })  # you need to export packages as well
  
  # Exporting the needed variables
  clusterExport(cl, varlist=c("tile_data","tile_data","tile_name_template","tile_folder_path","tile_id_name","proc_func","proc_func","fargs","get_rounded_extent","clip_convert","clip_inplace","env","res_l1","crs_lv95"), envir=environment())
  
  # Now processing...
  ret <- parallel::parLapply(cl=cl, X=seq(nrow(tile_data)),
                             fun = function(x) {
                               tile = tile_data[x,]
                               tile_id = tile_data[x,][[tile_id_name]]
                               tile_name <- paste0(tile_name_template,tile_id)
                               tile_output_path <- file.path(tile_folder,tile_name) 
                               tile_extent <- get_rounded_extent(tile,dem_res,max(tile_buffer,tile_output_buffer+100))
                               tile_output_extent <- get_rounded_extent(tile,25,tile_output_buffer)
                               #TODO: use lowest resolution (25m) and 50m buffer for output extent of all resolutions (rasters won't line up otherwise)
                               
                               return(proc_func(dem = dem, tile_extent = tile_extent, tile_output_extent = tile_output_extent, tile_folder = tile_output_path, tile_name_template = tile_name, tile_id = tile_id, fargs = fargs))
                             })
  stopCluster(cl)
  
  return(ret)
}

# A variant of the processing funtion, that processes the tiles in a linear fashion (handy for debugging)
process_dem_linear <- function(dem, tile_data, tile_folder, tile_name_template, tile_id_name, proc_func, tile_buffer = 500, tile_output_buffer = 100, n.cores = 25, fargs = NULL) {
  dem_res <- terra::res(rast(dem))[1]

  ret <- NA
  for(x in seq(nrow(tile_data))){
      

       tile = tile_data[x,]
       tile_id = tile_data[x,][[tile_id_name]]
       tile_name <- paste0(tile_name_template,tile_id)
       tile_output_path <- file.path(tile_folder,tile_name) 
       tile_extent <- get_rounded_extent(tile,dem_res,max(tile_buffer,tile_output_buffer+100))
       tile_output_extent <- get_rounded_extent(tile,dem_res,tile_output_buffer)
       print(paste(x,tile_id))
       
       t_ret <- proc_func(dem = dem, tile_extent = tile_extent, tile_output_extent = tile_output_extent, tile_folder = tile_output_path, tile_name_template = tile_name, tile_id = tile_id, fargs = fargs)
       ret <- rbind(ret, t_ret)
     
  }
  return(ret)
}

# Function for clipping a raster to a specified extent and saving the result as a GeoTIFF
clip_convert <- function(raster_path,tile_output_extent,delete_input=FALSE) {
  wd <- getwd()
  input_path <-sub(".",wd,gsub(".sgrd",".sdat",raster_path))
  output_path <-gsub(".sdat",".tif",input_path)
  
  if (!file.exists(output_path)){
    gdalwarp(srcfile=input_path,dstfile=output_path,t_srs=crs_lv95, overwrite=TRUE,co=c("TILED=YES","COMPRESS=DEFLATE"), r = "near", te = c(tile_output_extent[1],tile_output_extent[3],tile_output_extent[2],tile_output_extent[4]))
    #return(gdal_cmd)
  } else {return()}
  
  if(delete_input) {
    unlink(gsub(".sdat",".sgrd",input_path))
    unlink(gsub(".sdat",".mgrd",input_path))
    unlink(gsub(".sdat",".prj",input_path))
    unlink(gsub(".sdat",".sdat.aux.xml",input_path))
    unlink(input_path)
  }
  
  return(output_path)
}

# Function for clipping a raster to a specified extent overwriting the original raster
clip_inplace <- function(raster_path,tile_output_extent) {
  wd <- getwd()
  input_path <-sub(".",wd,raster_path)
  output_path <-gsub(".tif","_tmp.tif",input_path)
  
  print(input_path)
  print(output_path)
  
  if (!file.exists(output_path)){
    gdalwarp(srcfile=input_path,dstfile=output_path,t_srs=crs_lv95, overwrite=TRUE,co=c("TILED=YES","COMPRESS=DEFLATE"), r = "near", te = c(tile_output_extent[1],tile_output_extent[3],tile_output_extent[2],tile_output_extent[4]))
    #return(gdal_cmd)
  } else {return()}

  unlink(input_path)

  file.rename(output_path,input_path)
  return(output_path)
}

#
# Generate tiles for the raw DEM
#
clip_dem_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  dir.create(tile_folder)
  
  dem <- rast(dem)
  
  #Raw DEM tile with full buffer
  tile_output_name <- paste0(tile_name_template,"_raw.sdat")
  tile_output_path <- file.path(tile_folder, tile_output_name)
  
  if (!file.exists(tile_output_path)){
    dem_tile <- crop(dem,tile_extent,filename=tile_output_path,filetype = "SAGA", 
                     NAflag = NAflag(dem),overwrite=TRUE)
  }
  
  #DEM tile with output extent
  tile_output_name <- paste0(tile_name_template,".sdat")
  tile_output_path <- file.path(tile_folder, tile_output_name)
  
  if (!file.exists(tile_output_path)){
    dem_tile <- crop(dem,tile_output_extent,filename=tile_output_path,filetype = "SAGA", 
                     NAflag = NAflag(dem),overwrite=TRUE)
  }
  tile_output_path <- clip_convert(tile_output_path,tile_output_extent,delete_input=TRUE)
  
  return(tile_output_path)
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_dem_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = clip_dem_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = clip_dem_tiles, tile_buffer = 4000,  n.cores = n.cores)

#
# Generate pit filled Version of the DEM tiles
#
fill_dem_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  input_name <- paste0(tile_name_template,"_raw.sdat")
  tile_input_path <- file.path(tile_folder, input_name)
  
  dem_filled_path <-gsub("_raw.sdat","_filled.sgrd",tile_input_path)
  if (!file.exists(dem_filled_path)){
    rsaga.geoprocessor(lib = "ta_preprocessor", module = "Fill Sinks (Planchon/Darboux, 2001)",
                       param = list(DEM = gsub(".sdat",".sgrd",tile_input_path),
                                    RESULT = dem_filled_path),
                       env = env)
  }

  dem_filled_path <- clip_convert(dem_filled_path,tile_output_extent,delete_input=FALSE)
  
  
  
  return(dem_filled_path)
}


process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = fill_dem_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = fill_dem_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = fill_dem_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Generate tiles based on the SAGA "SAGA Wetness Index" tool
# Catchment area, Catchment Slope, TWI
#
saga_twi_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  
  tile_input_name <- paste0(tile_name_template,"_raw.sgrd")
  tile_input_path <- file.path(tile_folder, tile_input_name)
  
  catch_file_name <- file.path(tile_folder,paste0("catchment_",tile_name_template,".sgrd"))
  slope_file_name <- file.path(tile_folder,paste0("slope_twi_",tile_name_template,".sgrd"))
  twicatchmod_file_name <- file.path(tile_folder,paste0("TWIcatchmod_",tile_name_template,".sgrd"))
  twisaga_file_name <- file.path(tile_folder,paste0("TWIsaga_",tile_name_template,".sgrd"))
  
  
  if (!file.exists(catch_file_name)){
    rsaga.geoprocessor(lib = "ta_hydrology", module = "SAGA Wetness Index",
                       param = list(DEM = tile_input_path,
                                    AREA = catch_file_name,
                                    SLOPE = slope_file_name,
                                    AREA_MOD = twicatchmod_file_name,
                                    TWI = twisaga_file_name),
                       env = env)
  }
  
  
  catch_file_name <- clip_convert(catch_file_name,tile_output_extent,delete_input=TRUE)
  slope_file_name <- clip_convert(slope_file_name,tile_output_extent,delete_input=TRUE)
  twicatchmod_file_name <- clip_convert(twicatchmod_file_name,tile_output_extent,delete_input=TRUE)
  twisaga_file_name <- clip_convert(twisaga_file_name,tile_output_extent,delete_input=TRUE)
  
  return(c(catch_file_name,slope_file_name,twicatchmod_file_name,twisaga_file_name))
  
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = saga_twi_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = saga_twi_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = saga_twi_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Generate tiles based on the SAGA "Slope, Aspect, Curvature" tool
# Slope, Aspect, Genereal Curvature, Plan Curvature, Profile Curvature
#
slope_aspect_curv_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  
  tile_input_name <- paste0(tile_name_template,"_raw.sgrd")
  tile_input_path <- file.path(tile_folder, tile_input_name)
  
  # Diverse Indices, Slope, Aspect, Curvature VRM, TRI ----------
  slope_file_name <-file.path(tile_folder,paste0("slope_ind_",tile_name_template,".sgrd"))
  aspect_file_name <-file.path(tile_folder,paste0("aspect_",tile_name_template,".sgrd"))
  curv_file_name <-file.path(tile_folder,paste0("curv_",tile_name_template,".sgrd"))
  curvprov_file_name <-file.path(tile_folder,paste0("curvprov_",tile_name_template,".sgrd"))
  curvplan_file_name <-file.path(tile_folder,paste0("curvoplan_",tile_name_template,".sgrd"))
  
  if (!file.exists(slope_file_name)){
    rsaga.geoprocessor(lib = "ta_morphometry", module = "Slope, Aspect, Curvature",
                       param = list(ELEVATION = tile_input_path,
                                    SLOPE =  slope_file_name,
                                    ASPECT = aspect_file_name,
                                    C_GENE =  curv_file_name,
                                    C_PROF =  curvprov_file_name, 
                                    C_PLAN =  curvplan_file_name,
                                    UNIT_SLOPE = "1",
                                    UNIT_ASPECT = "1"),
                       env = env)  
  }
  
  
  slope_file_name <- clip_convert(slope_file_name,tile_output_extent,delete_input=TRUE)
  aspect_file_name <- clip_convert(aspect_file_name,tile_output_extent,delete_input=TRUE)
  curv_file_name <- clip_convert(curv_file_name,tile_output_extent,delete_input=TRUE)
  curvprov_file_name <- clip_convert(curvprov_file_name,tile_output_extent,delete_input=TRUE)
  curvplan_file_name <- clip_convert(curvplan_file_name,tile_output_extent,delete_input=TRUE)
  
  return(c(slope_file_name,aspect_file_name,curv_file_name,curvprov_file_name,curvplan_file_name))
  
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = slope_aspect_curv_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = slope_aspect_curv_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = slope_aspect_curv_tiles, tile_buffer = 500,  n.cores = n.cores)


#
# Generate tiles for positive and negative openness
#
openness_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  
  tile_input_name <- paste0(tile_name_template,"_raw.sgrd")
  tile_input_path <- file.path(tile_folder, tile_input_name)
  
  scale0 <- 20
  scale1 <- 50
  scale2 <- 200
  
  #Openness 20
  openness_pos_scale0_name <-file.path(tile_folder,paste0("openness_pos_",tile_name_template,"_",scale0,".sgrd"))
  openness_neg_scale0_name <-file.path(tile_folder,paste0("openness_neg_",tile_name_template,"_",scale0,".sgrd"))
  
  if (!file.exists(openness_pos_scale0_name)){
    rsaga.geoprocessor(lib = "ta_lighting", module = "Topographic Openness",
                       param = list(DEM = tile_input_path,
                                    POS =  openness_pos_scale0_name,
                                    NEG = openness_neg_scale0_name,
                                    RADIUS = scale0),
                       env = env)
  }
  
  openness_pos_scale0_name <- clip_convert(openness_pos_scale0_name,tile_output_extent,delete_input=TRUE)
  openness_neg_scale0_name <- clip_convert(openness_neg_scale0_name,tile_output_extent,delete_input=TRUE)
  
  
  #Openness 50
  openness_pos_scale1_name <-file.path(tile_folder,paste0("openness_pos_",tile_name_template,"_",scale1,".sgrd"))
  openness_neg_scale1_name <-file.path(tile_folder,paste0("openness_neg_",tile_name_template,"_",scale1,".sgrd"))
  if (!file.exists(openness_pos_scale1_name)){
    rsaga.geoprocessor(lib = "ta_lighting", module = "Topographic Openness",
                       param = list(DEM = tile_input_path,
                                    POS =  openness_pos_scale1_name,
                                    NEG = openness_neg_scale1_name,
                                    RADIUS = scale1),
                       env = env)
  }
  
  openness_pos_scale1_name <- clip_convert(openness_pos_scale1_name,tile_output_extent,delete_input=TRUE)
  openness_neg_scale1_name <- clip_convert(openness_neg_scale1_name,tile_output_extent,delete_input=TRUE)
  
  
  #Openness 200
  openness_pos_scale2_name <-file.path(tile_folder,paste0("openness_pos_",tile_name_template,"_",scale2,".sgrd"))
  openness_neg_scale2_name <-file.path(tile_folder,paste0("openness_neg_",tile_name_template,"_",scale2,".sgrd"))
  if (!file.exists(openness_pos_scale2_name)){
    rsaga.geoprocessor(lib = "ta_lighting", module = "Topographic Openness",
                       param = list(DEM = tile_input_path,
                                    POS =  openness_pos_scale2_name,
                                    NEG = openness_neg_scale2_name,
                                    RADIUS = scale2),
                       env = env)
  } 
  openness_pos_scale2_name <- clip_convert(openness_pos_scale2_name,tile_output_extent,delete_input=TRUE)
  openness_neg_scale2_name <- clip_convert(openness_neg_scale2_name,tile_output_extent,delete_input=TRUE)
  
  return(c(openness_pos_scale1_name,openness_neg_scale1_name,openness_pos_scale2_name,openness_neg_scale2_name))
  
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = openness_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Generate tiles based on the SAGA "Flow Accumulation (Top-Down)" tool
# Upstream Area/Flow Accumulation
#
upstream_area_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  input_name <- file.path(tile_folder,paste0(tile_name_template,"_filled.sgrd"))
  
  #Contributing Area
  upstream_area_name <- file.path(tile_folder,paste0("upstream_area_D8_",tile_name_template,".sgrd"))
  
  if (!file.exists(upstream_area_name)){
    rsaga.geoprocessor(lib = "ta_hydrology", module = "Flow Accumulation (Top-Down)",
                       param = list(ELEVATION = input_name,
                                    FLOW =  upstream_area_name,
                                    METHOD = 0),
                       env = env)
  }  
  upstream_area_name <- clip_convert(upstream_area_name,tile_output_extent,delete_input=TRUE)
  return(upstream_area_name)
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = upstream_area_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = upstream_area_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = upstream_area_tiles, tile_buffer = 500,  n.cores = n.cores)


#
# Generate tiles with the elevaton percentile
#
elev_percentile_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  
  terraOptions(memmax=5)
  
  circular_mask <- function(radius) {
    seq <- sequence(c(0, 2*radius+1))-(radius+1)
    mask <- abs(seq%*%t(seq)+seq%*%t(seq))<= radius*radius
    return(mask*1)
  }
  
  ep_func <- function(x) {
    center <- x[ceiling(length(x)/2)]
    
    if (is.na(center)) {
      return(center)
    } 
    
    ep <- sum(x>center)*100/length(x)
    return(ep)
  }
  
  input_raw_name <- file.path(tile_folder,paste0(tile_name_template,"_raw.sdat"))
  
  
  r.dem <- rast(input_raw_name) 
  
  r.dem <- rast(input_raw_name)
  dem_crs <- crs(r.dem)
  raster_resolution <- res(r.dem)[1]
  
  scale1 <- 50
  scale2 <- 200
  
  
  ep_scale1_name <-file.path(tile_folder,paste0("elevation_percentile_",tile_name_template,"_",scale1,".tif"))
  if (!file.exists(ep_scale1_name)){
    
    r_scale1 <- floor((scale1/raster_resolution)/2)
    mask_scale1 <- circular_mask(r_scale1)
    r.ep_scale1 <- focal(r.dem, w=mask_scale1, fun=ep_func, fillvalue=0)
    crs(r.ep_scale1) <- dem_crs
    writeRaster(r.ep_scale1, ep_scale1_name, filetype = "GTiff")
  }  
  ep_scale1_name <- clip_convert(ep_scale1_name,tile_output_extent,delete_input=TRUE)
  
  ep_scale2_name <-file.path(tile_folder,paste0("elevation_percentile_",tile_name_template,"_",scale2,".tif"))
  if (!file.exists(ep_scale2_name)){
    r_scale2 <- floor((scale2/raster_resolution)/2)
    mask_scale2 <- circular_mask(r_scale2)
    r.ep_scale2 <- focal(r.dem, w=mask_scale2, fun=ep_func, fillvalue=0)
    crs(r.ep_scale2) <- dem_crs
    writeRaster(r.ep_scale2, ep_scale2_name, filetype = "GTiff")
  }
  ep_scale2_name <- clip_convert(ep_scale2_name,tile_output_extent,delete_input=TRUE)
  
  return(c(ep_scale1_name,ep_scale2_name))
}

# The version with terra can cause memory problems in the parallelized version if no memory limit is set
process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = elev_percentile_tiles, tile_buffer = 500,  n.cores = n.cores)
#process_dem_linear(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = elev_percentile_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Generate tiles with the Topographic Position Index
#
tpi_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  input_name <- file.path(tile_folder,paste0(tile_name_template,"_raw.sdat"))
  
  terraOptions(memmax=4)
  
  r.dem <- rast(input_name) 
  dem_crs <- crs(r.dem)
  raster_resolution <- res(r.dem)[1]
  
  nrow <- fargs$nrow
  rast_out_template <- fargs$rast_out_template

  tpi_name <- file.path(tile_folder,paste0(rast_out_template,"_",tile_name_template,".tif"))
  
  if (!file.exists(tpi_name)){
    f <- matrix(1, nrow=nrow, ncol=nrow)
    tt <- ceiling(sum(f)/2)
           
    TPI <- focal(r.dem, w=f, fun=function(x, ...) x[tt] - mean(x[-tt]), fillvalue=NA,
                 filename = tpi_name, overwrite = TRUE, filetype = "GTiff", 
                 gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  }
  tpi_name <- clip_inplace(tpi_name,tile_output_extent)

  return(tpi_name)
  
}


tpi_func <- function(dem, nrow, tile_name_template, rast_out_template) {
  fargs <- list(nrow=nrow,rast_out_template=rast_out_template)
  process_dem_tiled(dem = dem, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = tpi_tiles, tile_buffer = 500,  n.cores = n.cores, fargs=fargs)
}

## Topographic Position Indices --- #Für jeden Punkt, Min/max als referenz--> Kuppe / Senke --> mit versch. r
#   Radii 15 m, 50 m, 200 m, 500 m, 1 km, 2 km, 4 km  

tpi_filename_15 <- "tpi_15m_"
tpi_filename_50 <- "tpi_50m_"
tpi_filename_200 <- "tpi_200m_"
tpi_filename_500 <- "tpi_500m_"
tpi_filename_1k <-  "tpi_1km_"
tpi_filename_2k <-  "tpi_2km_"
tpi_filename_4k <-  "tpi_4km_"

tpi_func(dem_resampled_file_name_l1,3,tile_name_template, tpi_filename_15)
tpi_func(dem_resampled_file_name_l2,5,tile_name_template_l2,tpi_filename_50)
tpi_func(dem_resampled_file_name_l2,21,tile_name_template_l2,tpi_filename_200)
tpi_func(dem_resampled_file_name_l3,21,tile_name_template_l3,tpi_filename_500)
tpi_func(dem_resampled_file_name_l3,41,tile_name_template_l3,tpi_filename_1k)
tpi_func(dem_resampled_file_name_l3,81,tile_name_template_l3,tpi_filename_2k)
tpi_func(dem_resampled_file_name_l3,161,tile_name_template_l3,tpi_filename_4k)


#
# Generate tiles based on the SAGA "Multiresolution Index of Valley Bottom Flatness (MRVBF)" tool
# MRVBF, MRRTF
#
mrvbf_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  input_name <- file.path(tile_folder,paste0(tile_name_template,"_raw.sgrd"))
  
  # Multiresolution Index of Valley Bottom Flatness (MRVBF) ----
  mrvb_name <-file.path(tile_folder,paste0("mrvbf_",tile_name_template,".sgrd"))
  mrrt_name <-file.path(tile_folder,paste0("mrrtf_",tile_name_template,".sgrd"))
  
  if (!file.exists(mrvb_name)){
    rsaga.geoprocessor(lib = "ta_morphometry", module = "Multiresolution Index of Valley Bottom Flatness (MRVBF)",
                       param = list(DEM = input_name, 
                                    MRVBF = mrvb_name,
                                    MRRTF = mrrt_name),
                       env = env)
  }
  mrvb_name <- clip_convert(mrvb_name,tile_output_extent,delete_input=TRUE)
  mrrt_name <- clip_convert(mrrt_name,tile_output_extent,delete_input=TRUE)
  return(c(mrvb_name,mrrt_name))
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = mrvbf_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = mrvbf_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = mrvbf_tiles, tile_buffer = 500,  n.cores = n.cores)


#
# Generate tiles based on the SAGA "Convergence Index" tool
# Convergence index
#
convergence_index_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  input_name <- file.path(tile_folder,paste0(tile_name_template,"_raw.sgrd"))
  
  
  # Convergence index
  cindex_name <-file.path(tile_folder,paste0("cindex_",tile_name_template,".sgrd"))
  if (!file.exists(cindex_name)){
    rsaga.geoprocessor(lib = "ta_morphometry", module = "Convergence Index",
                       param = list(ELEVATION = input_name, 
                                    RESULT = cindex_name,
                                    NEIGHBOURS = "3 x 3"),
                       env = env)
  }
  cindex_name <- clip_convert(cindex_name,tile_output_extent,delete_input=TRUE)
  return(cindex_name)
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = convergence_index_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = convergence_index_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = convergence_index_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Generate tiles based on the SAGA "Upslope and Downslope Curvature" tool
# Upsole Curvature, Downslope Curvature
#
upslope_downslope_curvature_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  input_name <- file.path(tile_folder,paste0(tile_name_template,"_raw.sgrd"))
  
  # Upslope and Downslope Curvature
  curvup_name <-file.path(tile_folder,paste0("curvup_",tile_name_template,".sgrd"))
  curvdown_name <-file.path(tile_folder,paste0("curvdown_",tile_name_template,".sgrd"))
  if (!file.exists(curvup_name)){
    rsaga.geoprocessor(lib = "ta_morphometry", module = "Upslope and Downslope Curvature",
                       param = list(DEM = input_name,
                                    C_UP = curvup_name,
                                    C_DOWN = curvdown_name),
                       env = env)
  }
  curvup_name <- clip_convert(curvup_name,tile_output_extent,delete_input=TRUE)
  curvdown_name <- clip_convert(curvdown_name,tile_output_extent,delete_input=TRUE)
  return(c(curvup_name,curvdown_name))
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = upslope_downslope_curvature_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = upslope_downslope_curvature_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = upslope_downslope_curvature_tiles, tile_buffer = 500,  n.cores = n.cores)


#
# Generate tiles based on the SAGA "Relative Heights and Slope Positions" tool
# Slope Height, Valley Depth, Normalized Height, Standardized Height, Mid-Slope Position
#
relative_heights_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  input_name <- file.path(tile_folder,paste0(tile_name_template,"_raw.sgrd"))
  
  # Relative Heights and Slope Positions
  
  heigslope_name <-file.path(tile_folder,paste0("heigslope_",tile_name_template,".sgrd"))
  heigval_name <-file.path(tile_folder,paste0("heigval_",tile_name_template,".sgrd"))
  heignor_name <-file.path(tile_folder,paste0("heignor_",tile_name_template,".sgrd"))
  heigstand_name <-file.path(tile_folder,paste0("heigstand_",tile_name_template,".sgrd"))
  heigmid_name <-file.path(tile_folder,paste0("heigmid_",tile_name_template,".sgrd"))
  
  if (!file.exists(heigslope_name)){
    rsaga.geoprocessor(lib = "ta_morphometry", module = "Relative Heights and Slope Positions",
                       param = list(DEM = input_name,
                                    HO = heigslope_name,
                                    HU = heigval_name,
                                    NH = heignor_name,
                                    SH = heigstand_name,
                                    MS = heigmid_name),
                       env = env)
  }  
  heigslope_name <- clip_convert(heigslope_name,tile_output_extent,delete_input=TRUE)
  heigval_name <- clip_convert(heigval_name,tile_output_extent,delete_input=TRUE)
  heignor_name <- clip_convert(heignor_name,tile_output_extent,delete_input=TRUE)
  heigstand_name <- clip_convert(heigstand_name,tile_output_extent,delete_input=TRUE)
  heigmid_name <- clip_convert(heigmid_name,tile_output_extent,delete_input=TRUE)
  return(c(heigslope_name,heigval_name,heignor_name,heigstand_name,heigmid_name))
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = relative_heights_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = relative_heights_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = relative_heights_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Generate tiles based on the SAGA "Morphometric Protection Index", "Vector Ruggedness Measure (VRM)", and "Terrain Ruggedness Index (TRI)" tools
# Morphometric Protection Index, Vector Ruggedness Index, Terrain Ruggedness Index
#
morphometric_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  input_name <- file.path(tile_folder,paste0(tile_name_template,"_raw.sgrd"))
  
  
  mpi_name <-file.path(tile_folder,paste0("mpi_r30_",tile_name_template,".sgrd"))
  vrm_name <-file.path(tile_folder,paste0("vrm_r5_",tile_name_template,".sgrd"))
  tri_name <-file.path(tile_folder,paste0("tri_r5_",tile_name_template,".sgrd"))
  
  if (!file.exists(mpi_name)){
    
    # Morphometric Protection Index
    rsaga.geoprocessor(lib = "ta_morphometry", module = "Morphometric Protection Index",
                       param = list(DEM = input_name,
                                    PROTECTION = mpi_name,
                                    RADIUS = 30),
                       env = env)
    
    # Vector Ruggedness Measure (VRM)
    rsaga.geoprocessor(lib = "ta_morphometry", module = "Vector Ruggedness Measure (VRM)",
                       param = list(DEM = input_name,
                                    VRM = vrm_name,
                                    RADIUS = 5),
                       env = env)
    
    # Terrain Ruggedness Index (TRI)
    rsaga.geoprocessor(lib = "ta_morphometry", module = "Terrain Ruggedness Index (TRI)",
                       param = list(DEM = input_name,
                                    TRI = tri_name,
                                    RADIUS = 5), # cells
                       env = env)
  }  
  mpi_name <- clip_convert(mpi_name,tile_output_extent,delete_input=TRUE)
  vrm_name <- clip_convert(vrm_name,tile_output_extent,delete_input=TRUE)
  tri_name <- clip_convert(tri_name,tile_output_extent,delete_input=TRUE)
  return(c(mpi_name,vrm_name,tri_name))
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = morphometric_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = morphometric_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = morphometric_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Generate tiles with Northness and Eastness of Aspect
#
northness_eastness_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {

  #Inputs are previously created tiles that have already been clipped and converted 
  input_name <-file.path(tile_folder,paste0("aspect_",tile_name_template,".tif"))
  
  nness_name <-file.path(tile_folder,paste0("asp_nness_",tile_name_template,".tif"))
  eness_name <-file.path(tile_folder,paste0("asp_eness_",tile_name_template,".tif"))
  
  
  if (!file.exists(nness_name)){
    
    # Eastness (West = -1, Ost = 1), Northness (S?d = -1, Nord = 1)
    aspect_raster <- rast(input_name)
    # To rad
    aspect_raster <- aspect_raster*pi/180
    
    eness <- sin(aspect_raster)
    nness <- cos(aspect_raster)    
    crop(eness,tile_output_extent,filename=eness_name,filetype = "GTiff", 
         NAflag = NAflag(aspect_raster),overwrite=TRUE, gdal=c("TILED=YES","COMPRESS=DEFLATE"))
    crop(nness,tile_output_extent,filename=nness_name,filetype = "GTiff", 
         NAflag = NAflag(aspect_raster),overwrite=TRUE, gdal=c("TILED=YES","COMPRESS=DEFLATE"))
    
  }
  return(c(eness_name,nness_name))
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = northness_eastness_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = northness_eastness_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = northness_eastness_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Generate tiles with Toposcale index
#
topscale_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  input_name <- file.path(tile_folder,paste0(tile_name_template,"_raw.sgrd"))
  input_name_sdat <-file_sdat <-gsub(".sgrd",".sdat",input_name)
  
  # Toposcale ------------
  # According to Nik Zimmermann, 1997, WSL. 
  # Quick translation into R. Not too nice. 
  # nam1 / HAFL, Apr. 2020
  
  # Toposcale parameters
  # t.srn <- 50  # The search radius of the smallest window (in units of CRS)
  # t.srx <- 2000  # The search radius of the largest window 
  # t.inc <- 200  # The increment to increase search radius 
  
  t.srn <- 5  # The search radius of the smallest window (in units of CRS)
  t.srx <- 185  # The search radius of the largest window
  t.inc <- 20  # The increment to increase search radius
  
  terraOptions(memmax=4)
  
  r.dem <- rast(input_name_sdat)
  
  t.outname <-file.path(tile_folder,paste0("toposc_",tile_name_template, t.srn, "_", t.srx, "_", t.inc,".tif"))
  
  #if (!file.exists(t.outname)){
    tmp.dir <- file.path(tile_folder,"tmp2")
    dir.create(tmp.dir)
    
    l.radius <- sort(seq(t.srn, t.srx, t.inc), decreasing = T)
    
    f.expgrid <- function(radius, dem = r.dem){
      t.n <- file.path(tmp.dir,paste0("topo-rad_",radius,".tif"))
      if(file.exists(t.n)){
        r.tpi <- rast(t.n) 
      } else {
        # tpi
        r.tpi <- dem - focal(dem, w = focalMat(dem, radius, type = "circle"), fun = sum, na.rm = T, normalizefillvalue=NA)
        # 
        print("pre")
        r.tpi <- (r.tpi - mean(values(r.tpi), na.rm = T))  / sd(values(r.tpi), na.rm = T) 
        
        #r.tpi <- (r.tpi - as.numeric(global(r.tpi, stat = 'mean', na.rm = T)) ) / as.numeric(global(r.tpi, stat = 'sd', na.rm = T)) 
        print("pre")
        writeRaster(r.tpi, file = t.n, filetype = "GTiff",overwrite=TRUE, gdal=c("TILED=YES","COMPRESS=DEFLATE"))
        
      }
      
      return(r.tpi)
    }
    
    ## pre-create in paralell
    #xx <- mclapply(l.radius, FUN = f.expgrid, mc.cores = length(l.radius))
    # pre-create (mcapply doesn't work on Windows, plus parallel in parallel would be bad) 
    xx <- lapply(l.radius, FUN = f.expgrid)
    
    
    # Create largest grid with Radius 
    r.larger <- f.expgrid(l.radius[1])
    
    
    for(rad in l.radius[-c(1)]){
      r.next <- f.expgrid(rad)
      t.ni <- file.path(tmp.dir,paste0("topo-rad_",rad,".tif"))
      if(file.exists(t.ni)){
        r.integ <- rast(t.ni)
      } else{
        r.integ <- overlay(r.larger, r.next, fun = function(r.l, r.s){ 
          return( ifelse( abs(r.s) > 120, r.s, r.l ) ) }
        )
        writeRaster(r.integ, file = t.ni, filetype = "GTiff")
      }
      r.larger <- r.integ
    }
    
    file.copy(t.ni, t.outname, overwrite=TRUE)
    t.outname <- clip_inplace(t.outname,tile_output_extent)
    return(t.outname)   
  #}  
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = topscale_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Cut tiles with geomorphon (needs to be processed with QGIS/GRASS beforehand)
#

geomorphon_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  
  tile_input_name <- paste0(tile_name_template,"_raw.sgrd")
  tile_input_path <- file.path(tile_folder, tile_input_name)
  
  scale1 <- 50
  scale2 <- 200
  
  dem_geomorphon_scale1_name <-file.path(tile_folder,paste0("geomorphon_",tile_name_template,"_",scale1,".sgrd"))
  dem_geomorphon_scale2_name <-file.path(tile_folder,paste0("geomorphon_",tile_name_template,"_",scale2,".sgrd"))
  

  #Geomorphon 50
  if (!file.exists(dem_geomorphon_scale1_name)){
    rsaga.geoprocessor(lib = "ta_lighting", module = "Geomorphons",
                       param = list(DEM = tile_input_path,
                                    GEOMORPHONS =  dem_geomorphon_scale1_name,
                                    RADIUS = scale1),
                       env = env)
  }
  
  dem_geomorphon_scale1_name <- clip_convert(dem_geomorphon_scale1_name,tile_output_extent,delete_input=TRUE)
  
  #Geomorphon 200
  if (!file.exists(dem_geomorphon_scale2_name)){
    rsaga.geoprocessor(lib = "ta_lighting", module = "Geomorphons",
                       param = list(DEM = tile_input_path,
                                    GEOMORPHONS =  dem_geomorphon_scale2_name,
                                    RADIUS = scale2),
                       env = env)
  }
  
  dem_geomorphon_scale2_name <- clip_convert(dem_geomorphon_scale2_name,tile_output_extent,delete_input=TRUE)
  
}
process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = geomorphon_tiles, tile_buffer = 500,  n.cores = n.cores)

#
# Generate tiles with Ground Cover based on swissTLM3D
#
tlm_gdb_path <- "E:/GIS_Projekte/Geodaten/swisstlm3d_2022-03_2056_5728.gdb/SWISSTLM3D_2022_LV95_LN02.gdb"
gc_layer_name <- "TLM_BODENBEDECKUNG"
target_res <- res(r.dem_l1)
target_extent <- extent(r.dem_l1)

# rasterize the vector ground cover
r.gc_raster_l1 <- file.path(getwd(),paste0("ground_cover_tlm_",res_l1,"m.tif"))
# gdal_cmd <- sprintf("%s gdal_rasterize -l %s -a OBJEKTART -tr %s %s -a_nodata 0 -te %s %s %s %s -ot Int16 -of GTiff %s %s ", gdal_path,gc_layer_name, target_res[1], target_res[2], target_extent[1] , target_extent[3], target_extent[2], target_extent[4], tlm_gdb_path, r.gc_raster_l1)
gdal_rasterize(src_datasource = tlm_gdb_path, dst_filename = r.gc_raster_l1, l = gc_layer_name, a = "OBJEKTART", tr=c(res_l1,res_l1), te = extent, ot = "Int16", of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"),dryrun=TRUE) 

clip_groundcover_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  dir.create(tile_folder)
  dem <- rast(dem)

  tile_output_name <- paste0("gc_",tile_name_template,".tif")
  tile_output_path <- file.path(tile_folder, tile_output_name)
  
  gc_tile <- crop(dem,tile_output_extent,filename=tile_output_path,filetype = "GTiff", 
                      NAflag = NAflag(dem),overwrite=TRUE, gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  
  return(tile_output_path)
}

process_dem_tiled(dem = r.gc_raster_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_groundcover_tiles, tile_buffer = 500,  n.cores = n.cores)

# Generate covariate tiles with 0/1 encoding for the ground cover classes
ground_cover_covariate_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  gc_in_name <- paste0("gc_",tile_name_template,".sdat")
  gc_in_path <- file.path(tile_folder, gc_in_name)
  
  r.gc <- rast(gc_in_path)
  
  out_paths <- c()
  levels <- c(1:15)
  
  for(level in levels){
    cv_name <- file.path(tile_folder,paste0("gc_",raster_resolution_l1,"f_",level))
    
    cv <- r.gc==level
    
    writeRaster(cv, cv_name, filetype = "GTiff", overwrite=TRUE, gdal=c("TILED=YES","COMPRESS=DEFLATE"))

    out_paths<-rbind(out_paths,cv_name)
  }
  return(out_paths)
}

process_dem_tiled(dem = r.gc_raster_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = ground_cover_covariate_tiles, tile_buffer = 500,  n.cores = n.cores)


#
# Generate tiles with information on geomorphology based on the GK500 Bedrock and Geocover Unconsolidated Deposits
#
gdalUtils::gdal_setInstallation(gdal_base_path)

#owgeo4w_shell_path table_name column_name, gpgk_path
add_attribute_template <- "ALTER TABLE %s ADD COLUMN %s INTEGER;"
#owgeo4w_shell_path table_name column_name default_value gpgk_path
initialize_attribute_template = "UPDATE %s SET %s=%s;"
#owgeo4w_shell_path table_name column_name new_value comparison_column comparison_value gpgk_path
recode_attribute_template <- "UPDATE %s SET %s=%s WHERE %s='%s';"
susceptibility_col_name <- "slide_susceptibility"
litho_id_col_name <- "litho_id"

deposits_path <- "E:/GIS_Projekte/Geodaten/Geocover/geocover_merged.gpkg"
deposits_layer <- "Unconsolidated_Deposits_PLG"

deposit_gpkg_path <- file.path(getwd(), "gc_unconsolidated_deposits.gpkg")

#Copy input into new GPKG
ogr2ogr(src_datasource_name = deposits_path, dst_datasource_name = deposit_gpkg_path,  nln = deposits_layer, layer = deposits_layer)

gdal_cmd <- sprintf(add_attribute_template, deposits_layer, susceptibility_col_name)
gdalUtils::ogrinfo(datasource_name = deposit_gpkg_path, dialect = "SQLite", sql = gdal_cmd)

gdal_cmd <- sprintf(add_attribute_template, deposits_layer, litho_id_col_name)
gdalUtils::ogrinfo(datasource_name = deposit_gpkg_path, dialect = "SQLite", sql = gdal_cmd)

gdal_cmd <- sprintf(initialize_attribute_template, deposits_layer, susceptibility_col_name, 1)
gdalUtils::ogrinfo(datasource_name = deposit_gpkg_path, dialect = "SQLite", sql = gdal_cmd)

gdal_cmd <- sprintf(initialize_attribute_template, deposits_layer, litho_id_col_name, 0)
gdalUtils::ogrinfo(datasource_name = deposit_gpkg_path, dialect = "SQLite", sql = gdal_cmd)

gk500_path <- "E:/GIS_Projekte/Geodaten/GK300_V1_3_DE/GK500_V1_3_DE/LV95/Shapes_LV95/PY_Basis_Flaechen_fixed.shp"
gk500_layer <- "PY_Basis_Flaechen"
gk500_gpkg_path <- file.path(getwd(), "gk500_basis_flaechen.gpkg")

#Copy input to new GPKG
ogr2ogr(src_datasource_name = gk500_path, dst_datasource_name = gk500_gpkg_path,  nln = gk500_layer, a_srs="EPSG:2056", config_options=c(SHAPE_ENCODING="UTF-8"), f="GPKG")

gdal_cmd <- sprintf(add_attribute_template, gk500_layer, susceptibility_col_name)
gdalUtils::ogrinfo(datasource_name = gk500_gpkg_path, dialect = "SQLite", sql = gdal_cmd)

gdal_cmd <- sprintf(add_attribute_template, gk500_layer, litho_id_col_name)
gdalUtils::ogrinfo(datasource_name = gk500_gpkg_path, dialect = "SQLite", sql = gdal_cmd)

gdal_cmd <- sprintf(initialize_attribute_template, gk500_layer, susceptibility_col_name, 1)
gdalUtils::ogrinfo(datasource_name = gk500_gpkg_path, dialect = "SQLite", sql = gdal_cmd)

gdal_cmd <- sprintf(initialize_attribute_template, gk500_layer, litho_id_col_name, 0)
gdalUtils::ogrinfo(datasource_name = gk500_gpkg_path, dialect = "SQLite", sql = gdal_cmd)


gc_dep_mappings_path <- "E:/GIS_Projekte/Paper_2/data/gc_unconsolidated_deposits_LITHO_mappings.csv"
deposits_layer <- "Unconsolidated_Deposits_PLG"
gc_dep_mappings <- read.csv(gc_dep_mappings_path,sep=";", encoding="UTF-8")


gk500_mappings_path <- "E:/GIS_Projekte/Paper_2/data/gk500_LITHO_mappings.csv"
gk500_layer <- "PY_Basis_Flaechen"
gk500_mappings <- read.csv(gk500_mappings_path,sep=";", encoding="UTF-8")

susceptibility_col_name <- "slide_susceptibility"
litho_id_col_name <- "litho_id"

recode_attribute_template <- "UPDATE %s SET %s=%s WHERE %s='%s';"

deposit_gpkg_path <- "E:/GIS_Projekte/Paper_2/data/geomorph/gc_unconsolidated_deposits.gpkg"
gk500_gpkg_path <- "E:/GIS_Projekte/Paper_2/data/geomorph/gk500_basis_flaechen.gpkg"

for (i in 1:nrow(gc_dep_mappings)) {
  rec <- gc_dep_mappings[i,]
  litho <- rec["LITHO"]
  susceptibility <- rec["Slide_Susceptibility"]
  litho_id <- rec["Litho_ID"]
  
  
  rc_cmd <- sprintf(recode_attribute_template,deposits_layer, susceptibility_col_name, susceptibility, "LITHO", litho)
  gdalUtils::ogrinfo(datasource_name = deposit_gpkg_path, dialect = "SQLite", sql = rc_cmd)

}

for (i in 1:nrow(gc_dep_mappings)){
  rec <- gc_dep_mappings[i,]
  litho <- rec["LITHO"]
  susceptibility <- rec["Slide_Susceptibility"]
  litho_id <- rec["Litho_ID"]
  
  rc_cmd <- sprintf(recode_attribute_template,deposits_layer, litho_id_col_name, litho_id, "LITHO", litho)
  gdalUtils::ogrinfo(datasource_name = deposit_gpkg_path, dialect = "SQLite", sql = rc_cmd)
}


for (i in 1:nrow(gk500_mappings)){
  rec <- gk500_mappings[i,]
  litho <- rec["LITHO"]
  susceptibility <- rec["Slide_Susceptibility"]
  litho_id <- rec["Litho_ID"]
  
  rc_cmd <- sprintf(recode_attribute_template,gk500_layer, susceptibility_col_name, susceptibility, "LITHO", litho)
  gdalUtils::ogrinfo(datasource_name = gk500_gpkg_path, dialect = "SQLite", sql = rc_cmd)
  
}

for (i in 1:nrow(gk500_mappings)){
  rec <- gk500_mappings[i,]
  litho <- rec["LITHO"]
  susceptibility <- rec["Slide_Susceptibility"]
  litho_id <- rec["Litho_ID"]
  
  
  rc_cmd <- sprintf(recode_attribute_template,gk500_layer, litho_id_col_name, litho_id, "LITHO", litho)
  gdalUtils::ogrinfo(datasource_name = gk500_gpkg_path, dialect = "SQLite", sql = rc_cmd)
  
}

target_res <- res(r.dem_l1)
target_extent <- ext(r.dem_l1)

#  Rasterize the inputs
rasterize_template <- "%s gdal_rasterize -a %s -tr %s %s -te %s %s %s %s -ot Byte -init %s -a_nodata 0 -co COMPRESS=LZW %s  %s"

r.deposits_l1 <- file.path(getwd(),paste0("unconsolidated_deposits_susceptibility_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = deposit_gpkg_path, dst_filename = r.deposits_l1,
               a = susceptibility_col_name, tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Byte", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 

r.deposits_id_l1 <- file.path(getwd(),paste0("unconsolidated_deposits_id_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = deposit_gpkg_path, dst_filename = r.deposits_id_l1,
               a = litho_id_col_name, tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Int32", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 

r.gk500_l1 <- file.path(getwd(),paste0("gk500_susceptibility_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = gk500_gpkg_path, dst_filename = r.gk500_l1,
               a = susceptibility_col_name, tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Byte", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 

r.gk500_id_l1 <- file.path(getwd(),paste0("gk500_id_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = gk500_gpkg_path, dst_filename = r.gk500_id_l1,
               a = litho_id_col_name, tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Int32", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = TRUE ) 

clip_deposit_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  dir.create(tile_folder)
  dem <- rast(dem)
  tile_output_name <- paste0("unconsolidated_deposits_",fargs,"_",tile_name_template,".tiff")
  tile_output_path <- file.path(tile_folder, tile_output_name)
  dep_tile <- crop(dem,tile_output_extent,filename=tile_output_path, filetype = "GTiff", 
                   NAflag = NAflag(dem),overwrite=TRUE, gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  return(tile_output_path)
}

process_dem_tiled(dem = r.deposits_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_deposit_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "susceptibility")
process_dem_tiled(dem = r.deposits_id_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_deposit_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "id")

clip_gk500_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  dir.create(tile_folder)
  dem <- rast(dem)
  tile_output_name <- paste0("gk500_",fargs,"_",tile_name_template,".tiff")
  tile_output_path <- file.path(tile_folder, tile_output_name)
  dep_tile <- crop(dem,tile_output_extent,filename=tile_output_path,filetype = "GTiff", 
                   NAflag = NAflag(dem),overwrite=TRUE, gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  
  return(tile_output_path)
}

process_dem_tiled(dem = r.gk500_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_gk500_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "susceptibility")
process_dem_tiled(dem = r.gk500_id_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_gk500_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "id")

susceptibility_covariate_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  sus_in_name <- fargs["sus_in_name"] 
  sus_in_name <- gsub("<tilename>",tile_name_template,sus_in_name)
  sus_in_path <- file.path(tile_folder, sus_in_name)
  
  r.sus <- rast(sus_in_path)

  out_paths <- c()
  levels <- c(0:3)

  for(level in levels){
    cv_name <- gsub(".tiff",paste0("f_",level,".tif"),sus_in_path)

    cv <- r.sus==level

    writeRaster(cv, cv_name, filetype = "GTiff", overwrite=TRUE, datatype="INT2S", gdal=c("TILED=YES","COMPRESS=DEFLATE"))

    out_paths<-rbind(out_paths,cv_name)
  }
  return(out_paths)
}

fargs <- list(sus_in_name = "unconsolidated_deposits_susceptibility_<tilename>.tiff")
process_dem_tiled(dem = r.deposits_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = susceptibility_covariate_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = fargs)
fargs <- list(sus_in_name = "gk500_susceptibility_<tilename>.tiff")
process_dem_tiled(dem = r.gk500_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = susceptibility_covariate_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = fargs)


#
# Generate tiles based on the crown height model
#
r.vhm <- "E:/GIS_Projekte/FINT-CH/VHM_2021-08-12.tif"

# r.vhm <- raster(r.vhm)
h_vhm_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  dir.create(tile_folder)
  
  #dem <- rast(dem)

  vhm_output_name <- paste0("vhm_",tile_name_template,"1m.tif")
  vhm_output_path <- file.path(tile_folder, vhm_output_name)

  extent_buf <- tile_extent+100

  gdalwarp(srcfile=normalizePath(dem),dstfile=normalizePath(vhm_output_path),overwrite=TRUE, of="GTiff",te=c(extent_buf[1] , extent_buf[3], extent_buf[2], extent_buf[4]),co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"))
  
    
  #r.vhm_1m <- crop(dem,tile_extent+100,filename=vhm_output_path,filetype = "GTiff", NAflag = NAflag(dem),overwrite=TRUE, gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  
  target_res <- fargs
  
  gdal_path <- "C:/OSGeo4W64/OSGeo4W.bat  "
  gdal_cmd_template <- "%s gdalwarp -overwrite -tr %s %s  -co COMPRESS=LZW -dstnodata 0 -te %s %s %s %s -r %s -of GTiff %s %s "
  
  vhm_max_output_name <- paste0("vhm_max_",tile_name_template,".tif")
  vhm_max_output_path <- file.path(tile_folder, vhm_max_output_name)
  # gdal_cmd_max <- sprintf(gdal_cmd_template, gdal_path, target_res, target_res, tile_extent[1] , tile_extent[3], tile_extent[2], tile_extent[4], "max", normalizePath(vhm_output_path), normalizePath(vhm_max_output_path))
  gdalwarp(srcfile=normalizePath(vhm_output_path),dstfile=normalizePath(vhm_max_output_path),of="GTiff",tr=c(target_res,target_res),te=c(tile_extent[1] , tile_extent[3], tile_extent[2], tile_extent[4]),r="max",overwrite = TRUE,co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"))
  
  vhm_q3_output_name <- paste0("vhm_q3_",tile_name_template,".tif")
  vhm_q3_output_path <- file.path(tile_folder, vhm_q3_output_name)
  # gdal_cmd_q3 <- sprintf(gdal_cmd_template, gdal_path, target_res, target_res, tile_extent[1] , tile_extent[3], tile_extent[2], tile_extent[4], "q3", normalizePath(vhm_output_path), normalizePath(vhm_q3_output_path))
  gdalwarp(srcfile=normalizePath(vhm_output_path),dstfile=normalizePath(vhm_q3_output_path),of="GTiff",tr=c(target_res,target_res),te=c(tile_extent[1] , tile_extent[3], tile_extent[2], tile_extent[4]),r="q3",overwrite = TRUE,co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"))
  
  unlink(vhm_output_path)
  
  clip_inplace(vhm_max_output_path,tile_output_extent)
  clip_inplace(vhm_q3_output_path,tile_output_extent)
  
  return(c(vhm_max_output_path,vhm_q3_output_path))
}

process_dem_tiled(dem = r.vhm, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = h_vhm_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = res_l1)
process_dem_tiled(dem = r.vhm, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = h_vhm_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = res_l2)
process_dem_tiled(dem = r.vhm, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = h_vhm_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = res_l3)

#
# Generate tiles based on the SAGA "Topographic Wetness Index (TWI)" tool
# TWI
#
twi_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  
  tile_input_name <- paste0(tile_name_template,"_raw.sgrd")
  tile_input_path <- file.path(tile_folder, tile_input_name)
  
  slope_input_path <-normalizePath(file.path(tile_folder,paste0("slope_ind_",tile_name_template,".tif")))
  slope_input_path_saga <- gsub(".tif",".sdat",slope_input_path)
  
  #Flow Accumulation
  catch_file_input_path <- normalizePath(file.path(tile_folder,paste0("upstream_area_D8_",tile_name_template,".tif"))) 
  #SAGA TWI
  catch_file_input_path <- normalizePath(file.path(tile_folder,paste0("catchment_",tile_name_template,".tif")))
  catch_file_input_path_saga <- gsub(".tif",".sdat",catch_file_input_path)
  
  gdal_translate(slope_input_path,slope_input_path_saga)
  gdal_translate(catch_file_input_path,catch_file_input_path_saga)
  
  twi_file_name <- file.path(tile_folder,paste0("TWIori_",tile_name_template,".sgrd"))
  
  if (!file.exists(twi_file_name)){
    
    rsaga.geoprocessor(lib = "ta_hydrology", module = "Topographic Wetness Index (TWI)",
                       param = list(SLOPE = gsub(".sdat",".sgrd",slope_input_path_saga),
                                    AREA = gsub(".sdat",".sgrd",catch_file_input_path_saga),
                                    TWI = twi_file_name,
                                    CONV = 0,
                                    METHOD = 0),
                       env = env)
  }
  
  for(f in c(slope_input_path_saga,catch_file_input_path_saga)) {
      unlink(gsub(".sdat",".sgrd",f))
      unlink(gsub(".sdat",".mgrd",f))
      unlink(gsub(".sdat",".prj",f))
      unlink(gsub(".sdat",".sdat.aux.xml",f))
      unlink(f)
  }
  
  twi_file_name <- clip_convert(twi_file_name,tile_output_extent,delete_input=TRUE)
  
  return(c(twi_file_name))
  
}

process_dem_tiled(dem = dem_resampled_file_name_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = twi_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l2, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l2, tile_id_name = tile_id_name, proc_func = twi_tiles, tile_buffer = 500,  n.cores = n.cores)
process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = twi_tiles, tile_buffer = 500,  n.cores = n.cores)


#
# Derive tiles for Percipitation cenarios from HADES
#
clip_percipitation_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  dir.create(tile_folder)

  prefix <- fargs$prefix
  input_path <- fargs$input_path
  input_path <- normalizePath(fargs$input_path)
  output_resolution <- fargs$output_resolution
  tile_output_name <- paste0(prefix,tile_name_template,".tif")
  tile_output_path <- normalizePath(file.path(tile_folder, tile_output_name))

  gdalwarp(srcfile=input_path,dstfile=tile_output_path,t_srs=crs_lv95, overwrite=TRUE,co=c("TILED=YES","COMPRESS=DEFLATE"), r = "near", tr=c(output_resolution,output_resolution), te = c(tile_output_extent[1],tile_output_extent[3],tile_output_extent[2],tile_output_extent[4]))
  
  return(tile_output_path)
}

raster_list <- list(
  "prcp_60m_2a" = "../HADES_B04/xspace_data_for_hades_60m_2J_05_.tif",
  "prcp_60m_2a_0025" = "../HADES_B04/xspace_data_for_hades_60m_2J_0025_.tif",
  "prcp_60m_2a_0975" = "../HADES_B04/xspace_data_for_hades_60m_2J_0975_.tif",
  "prcp_60m_10a" = "../HADES_B04/xspace_data_for_hades_60m_10J_05_.tif",
  "prcp_60m_10a_0025" = "../HADES_B04/xspace_data_for_hades_60m_10J_0025_.tif",
  "prcp_60m_10a_0975" = "../HADES_B04/xspace_data_for_hades_60m_10J_0975_.tif",
  "prcp_60m_30a" = "../HADES_B04/xspace_data_for_hades_60m_30J_05_.tif",
  "prcp_60m_30a_0025" = "../HADES_B04/xspace_data_for_hades_60m_30J_0025_.tif",
  "prcp_60m_30a_0975" = "../HADES_B04/xspace_data_for_hades_60m_30J_0975_.tif",
  "prcp_60m_50a" = "../HADES_B04/xspace_data_for_hades_60m_50J_05_.tif",
  "prcp_60m_50a_0025" = "../HADES_B04/xspace_data_for_hades_60m_50J_0025_.tif",
  "prcp_60m_50a_0975" = "../HADES_B04/xspace_data_for_hades_60m_50J_0975_.tif",
  "prcp_60m_100a" = "../HADES_B04/xspace_data_for_hades_60m_100J_05_.tif",
  "prcp_60m_100a_0025" = "../HADES_B04/xspace_data_for_hades_60m_100J_0025_.tif",
  "prcp_60m_100a_0975" = "../HADES_B04/xspace_data_for_hades_60m_100J_0975_.tif",
  "prcp_60m_200a" = "../HADES_B04/xspace_data_for_hades_60m_200J_05_.tif",
  "prcp_60m_200a_0025" = "../HADES_B04/xspace_data_for_hades_60m_200J_0025_.tif",
  "prcp_60m_200a_0975" = "../HADES_B04/xspace_data_for_hades_60m_200J_0975_.tif",  
  "prcp_60m_300a" = "../HADES_B04/xspace_data_for_hades_60m_300J_05_.tif",
  "prcp_60m_300a_0025" = "../HADES_B04/xspace_data_for_hades_60m_300J_0025_.tif",
  "prcp_60m_300a_0975" = "../HADES_B04/xspace_data_for_hades_60m_300J_0975_.tif",
  "prcp_24h_2a" = "../HADES_B04/xspace_data_for_hades_24h_2J_05_.tif",
  "prcp_24h_2a_0025" = "../HADES_B04/xspace_data_for_hades_24h_2J_0025_.tif",
  "prcp_24h_2a_0975" = "../HADES_B04/xspace_data_for_hades_24h_2J_0975_.tif",
  "prcp_24h_10a" = "../HADES_B04/xspace_data_for_hades_24h_10J_05_.tif",
  "prcp_24h_10a_0025" = "../HADES_B04/xspace_data_for_hades_24h_10J_0025_.tif",
  "prcp_24h_10a_0975" = "../HADES_B04/xspace_data_for_hades_24h_10J_0975_.tif",
  "prcp_24h_30a" = "../HADES_B04/xspace_data_for_hades_24h_30J_05_.tif",
  "prcp_24h_30a_0025" = "../HADES_B04/xspace_data_for_hades_24h_30J_0025_.tif",
  "prcp_24h_30a_0975" = "../HADES_B04/xspace_data_for_hades_24h_30J_0975_.tif",
  "prcp_24h_50a" = "../HADES_B04/xspace_data_for_hades_24h_50J_05_.tif",
  "prcp_24h_50a_0025" = "../HADES_B04/xspace_data_for_hades_24h_50J_0025_.tif",
  "prcp_24h_50a_0975" = "../HADES_B04/xspace_data_for_hades_24h_50J_0975_.tif",
  "prcp_24h_100a" = "../HADES_B04/xspace_data_for_hades_24h_100J_05_.tif",
  "prcp_24h_100a_0025" = "../HADES_B04/xspace_data_for_hades_24h_100J_0025_.tif",
  "prcp_24h_100a_0975" = "../HADES_B04/xspace_data_for_hades_24h_100J_0975_.tif",
  "prcp_24h_200a" = "../HADES_B04/xspace_data_for_hades_24h_200J_05_.tif",
  "prcp_24h_200a_0025" = "../HADES_B04/xspace_data_for_hades_24h_200J_0025_.tif",
  "prcp_24h_200a_0975" = "../HADES_B04/xspace_data_for_hades_24h_200J_0975_.tif",  
  "prcp_24h_300a" = "../HADES_B04/xspace_data_for_hades_24h_300J_05_.tif",
  "prcp_24h_300a_0025" = "../HADES_B04/xspace_data_for_hades_24h_300J_0025_.tif",
  "prcp_24h_300a_0975" = "../HADES_B04/xspace_data_for_hades_24h_300J_0975_.tif"
)

for(n in names(raster_list)){
  input_path <- raster_list[[n]]
  fargs <- list(prefix=n,input_path=input_path,output_resolution=res_l3)
  res <- process_dem_tiled(dem = dem_resampled_file_name_l3, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template_l3, tile_id_name = tile_id_name, proc_func = clip_percipitation_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = fargs)
  print(res)
}
  


#
# Generate tiles for Rockdensity
#
gdalUtils::gdal_setInstallation(gdal_base_path)

#owgeo4w_shell_path table_name column_name, gpgk_path
add_attribute_template <- "ALTER TABLE %s ADD COLUMN %s INTEGER;"
#owgeo4w_shell_path table_name column_name default_value gpgk_path
initialize_attribute_template = "UPDATE %s SET %s=%s;"
#owgeo4w_shell_path table_name column_name new_value comparison_column comparison_value gpgk_path
recode_attribute_template <- "UPDATE %s SET %s=%s WHERE %s='%s';"
saphyr_id_col_name <- "saphyr_id"

rd_path <- "E:/GIS_Projekte/Geodaten/ch.swisstopo.geologie-gesteinsdichte/Gesteinsdichte.shp"
rd_layer <- "rockdensity"

rd_gpkg_path <- file.path(getwd(), "geo_rockdensity.gpkg")

#Copy input into new GPKG
ogr2ogr(src_datasource_name = rd_path, dst_datasource_name = rd_gpkg_path, t_srs="EPSG:2056", nln = rd_layer, nlt = "PROMOTE_TO_MULTI", overwrite = TRUE)

gdal_cmd <- sprintf(add_attribute_template, rd_layer, saphyr_id_col_name)
gdalUtils::ogrinfo(datasource_name = rd_gpkg_path, dialect = "SQLite", sql = gdal_cmd)

gdal_cmd <- sprintf(initialize_attribute_template, rd_layer, saphyr_id_col_name, 1)
gdalUtils::ogrinfo(datasource_name = rd_gpkg_path, dialect = "SQLite", sql = gdal_cmd)

rd_mappings_path <- "E:/GIS_Projekte/Paper_2/data/geo_density_saphyr_n_mappings.csv"
rd_layer <- "rockdensity"
rd_mappings <- read.csv(rd_mappings_path,sep=";", fileEncoding="UTF-8-BOM",encoding="UTF-8")
rd_mappings
saphyr_n_col_name <- "SAPHYR_N"
saphyr_id_col_name <- "saphyr_id"

recode_attribute_template <- "UPDATE %s SET %s=%s WHERE %s='%s';"

for (i in 1:nrow(rd_mappings)) {
  rec <- rd_mappings[i,]
  saphyr_n <- rec["SAPHYR_N"]
  saphyr_id <- rec["saphyr_id"]
  
  rc_cmd <- sprintf(recode_attribute_template,rd_layer, saphyr_id_col_name, saphyr_id, "SAPHYR_N", saphyr_n)
  gdalUtils::ogrinfo(datasource_name = rd_gpkg_path, dialect = "SQLite", sql = rc_cmd)
  
}

target_res <- res(r.dem_l1)
target_extent <- ext(r.dem_l1)

# Rasterize the inputs
rasterize_template <- "%s gdal_rasterize -a %s -tr %s %s -te %s %s %s %s -ot Byte -init %s -a_nodata 0 -co COMPRESS=LZW %s  %s"

r.rd_l1 <- file.path(getwd(),paste0("rockdensity_litho_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = rd_gpkg_path, dst_filename = r.rd_l1,
               a = saphyr_id_col_name, tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Byte", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 


r.rd2_l1 <- file.path(getwd(),paste0("rhob_m_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = rd_gpkg_path, dst_filename = r.rd2_l1,
               a = "RHOB_M", tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Int32", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 

r.rd3_l1 <- file.path(getwd(),paste0("rhob_med_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = rd_gpkg_path, dst_filename = r.rd3_l1,
               a = "RHOB_MED", tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Int32", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 

r.rd4_l1 <- file.path(getwd(),paste0("rhob_p05_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = rd_gpkg_path, dst_filename = r.rd4_l1,
               a = "RHOB_p05", tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Int32", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 

r.rd5_l1 <- file.path(getwd(),paste0("rhob_p25_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = rd_gpkg_path, dst_filename = r.rd5_l1,
               a = "RHOB_p25", tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Int32", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 

r.rd6_l1 <- file.path(getwd(),paste0("rhob_p75_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = rd_gpkg_path, dst_filename = r.rd6_l1,
               a = "RHOB_p75", tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Int32", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 


r.rd7_l1 <- file.path(getwd(),paste0("rhob_p95_",res_l1,"m.tif"))
gdal_rasterize(src_datasource = rd_gpkg_path, dst_filename = r.rd7_l1,
               a = "RHOB_p95", tr=c(target_res[1],target_res[1]), te = c(target_extent[1] , target_extent[3], target_extent[2], target_extent[4]),  ot = "Int32", init=0,
               of = "GTiff", co=c("TILED=YES","BIGTIFF=YES","COMPRESS=DEFLATE"), dryrun = FALSE ) 


clip_rd_tiles <- function (dem, tile_extent, tile_output_extent, tile_folder, tile_name_template, tile_id, fargs) {
  dir.create(tile_folder)
  dem <- rast(dem)
  
  tile_output_name <- paste0(fargs,"_",tile_name_template,".tiff")
  tile_output_path <- file.path(tile_folder, tile_output_name)
  dep_tile <- crop(dem,tile_output_extent,filename=tile_output_path, filetype = "GTiff", 
                   NAflag = NAflag(dem),overwrite=TRUE, gdal=c("TILED=YES","COMPRESS=DEFLATE"))
  return(tile_output_path)
  
  
  
  
}

process_dem_tiled(dem = r.rd_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_rd_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "rockdensity_litho_")
process_dem_tiled(dem = r.rd2_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_rd_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "rhob_m_")
process_dem_tiled(dem = r.rd3_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_rd_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "rhob_med_")
process_dem_tiled(dem = r.rd4_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_rd_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "rhob_p05_")
process_dem_tiled(dem = r.rd5_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_rd_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "rhob_p25_")
process_dem_tiled(dem = r.rd6_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_rd_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "rhob_p75_")
process_dem_tiled(dem = r.rd7_l1, tile_data = tile_data, tile_folder = tile_folder_path, tile_name_template = tile_name_template, tile_id_name = tile_id_name, proc_func = clip_rd_tiles, tile_buffer = 500,  n.cores = n.cores, fargs = "rhob_p95_")

