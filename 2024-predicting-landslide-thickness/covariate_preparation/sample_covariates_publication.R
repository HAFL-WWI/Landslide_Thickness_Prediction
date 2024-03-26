######################################################################
# Copyright (C) 2024 BFH
#
# Script for sampling the prevoiusly generated the covariate rasters  
# at the slide failure points.
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

slide_path <-"../hangmuren-export-2023-04-10-10-06.csv"
storme_slide_path <-"../storme_interlis_merged_selection.csv"

res_l1 <- 5
res_l2 <- 10
res_l3 <- 25

scale0 <- 20
scale1 <- 50
scale2 <- 200

setwd(output_base_path)

output_base_path <- "."
n.cores<-20

tile_folder <- "ch_tiles"
tile_name_template <- paste0("dem_tile_",res_l1,"_ezg")
tile_name_template_l2 <- paste0("dem_tile_",res_l2,"_ezg")
tile_name_template_l3 <- paste0("dem_tile_",res_l3,"_ezg")
tile_id_name <- "TEZGNR150"
#tile_data <- watersheds.filtered

tile_folder_path <- file.path(output_base_path,tile_folder) 
dir.create(tile_folder_path,showWarnings =FALSE)

# Function for sampling values at points from a raster
sample_raster <- function(df,tile_output_folder,raster_name,raster_extension) {
  rast.path <- file.path(tile_output_folder,paste0(raster_name,".",raster_extension))
  print(rast.path)
  
  sample.rast <- rast(rast.path)
  crs(sample.rast) <- crs(df)

  sample.values <- terra::extract(sample.rast,df,fun=max,raw=TRUE)

  return(sample.values[,names(sample.rast)[[1]]])
}

# Function for sampling values at points from vector polygons
sample_vector <- function(df,tile_output_folder,vector_name,vector_extension,field_name) {
  vect.path <- file.path(tile_output_folder,paste0(vector_name,".",vector_extension))
  print(vect.path)
  
  sample.vect <- st_read(vect.path)
  
  sample.values <- st_join(x = df, y = sample.vect)
  
  return(sample.values[,field_name])
}

# Function for sampling all points within a specific tile/catchment
process_tile <- function(tile_id) {
  print(tile_id)
  hm.ezg <- hm.joined[hm.joined[[tile_id_name]]==tile_id,]
  
  tile_name_l1 <- paste0(tile_name_template,tile_id)
  tile_name_l2 <- paste0(tile_name_template_l2,tile_id)
  tile_name_l3 <- paste0(tile_name_template_l3,tile_id)
  
  tile_output_folder_l1 <- file.path(tile_folder,tile_name_l1) 
  tile_output_folder_l2 <- file.path(tile_folder,tile_name_l2)   
  tile_output_folder_l3 <- file.path(tile_folder,tile_name_l3)   
  
  #Raw DEM height above sea level
  raster_name_l1 <- tile_name_l1
  hm.ezg[paste0("h_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- tile_name_l2
  hm.ezg[paste0("h_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- tile_name_l3
  hm.ezg[paste0("h_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Filled DEM height above sea level
  raster_name_l1 <- paste0(tile_name_l1,"_filled")
  hm.ezg[paste0("h_filled_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0(tile_name_l2,"_filled")
  hm.ezg[paste0("h_filled_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0(tile_name_l3,"_filled")
  hm.ezg[paste0("h_filled_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Catchement Area in cells
  raster_name_l1 <- paste0("catchment_",tile_name_l1)
  hm.ezg[paste0("catchment_area_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("catchment_",tile_name_l2)
  hm.ezg[paste0("catchment_area_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("catchment_",tile_name_l3)
  hm.ezg[paste0("catchment_area_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Catchment slope
  raster_name_l1 <- paste0("slope_twi_",tile_name_l1)
  hm.ezg[paste0("catchment_slope_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("slope_twi_",tile_name_l2)
  hm.ezg[paste0("catchment_slope_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("slope_twi_",tile_name_l3)
  hm.ezg[paste0("catchment_slope_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Modified Catchment Area
  raster_name_l1 <- paste0("TWIcatchmod_",tile_name_l1)
  hm.ezg[paste0("modified_catchement_area_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("TWIcatchmod_",tile_name_l2)
  hm.ezg[paste0("modified_catchement_area_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("TWIcatchmod_",tile_name_l3)
  hm.ezg[paste0("modified_catchement_area_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Topographic Wetness Index SAGA
  raster_name_l1 <- paste0("TWIsaga_",tile_name_l1)
  hm.ezg[paste0("twi_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("TWIsaga_",tile_name_l2)
  hm.ezg[paste0("twi_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("TWIsaga_",tile_name_l3)
  hm.ezg[paste0("twi_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Topographic Wetness Index
  raster_name_l1 <- paste0("TWIori_",tile_name_l1)
  hm.ezg[paste0("twi_ori_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("TWIori_",tile_name_l2)
  hm.ezg[paste0("twi_ori_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("TWIori_",tile_name_l3)
  hm.ezg[paste0("twi_ori_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Slope
  raster_name_l1 <- paste0("slope_ind_",tile_name_l1)
  hm.ezg[paste0("slope_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("slope_ind_",tile_name_l2)
  hm.ezg[paste0("slope_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("slope_ind_",tile_name_l3)
  hm.ezg[paste0("slope_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Aspect
  raster_name_l1 <- paste0("aspect_",tile_name_l1)
  hm.ezg[paste0("aspect_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("aspect_",tile_name_l2)
  hm.ezg[paste0("aspect_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("aspect_",tile_name_l3)
  hm.ezg[paste0("aspect_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #General Curvature
  raster_name_l1 <- paste0("curv_",tile_name_l1)
  hm.ezg[paste0("curvature_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("curv_",tile_name_l2)
  hm.ezg[paste0("curvature_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("curv_",tile_name_l3)
  hm.ezg[paste0("curvature_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Profile Curvature
  raster_name_l1 <- paste0("curvprov_",tile_name_l1)
  hm.ezg[paste0("curvature_profile_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("curvprov_",tile_name_l2)
  hm.ezg[paste0("curvature_profile_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("curvprov_",tile_name_l3)
  hm.ezg[paste0("curvature_profile_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Plan Curvature
  raster_name_l1 <- paste0("curvoplan_",tile_name_l1)
  hm.ezg[paste0("curvature_planar_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("curvoplan_",tile_name_l2)
  hm.ezg[paste0("curvature_planar_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("curvoplan_",tile_name_l3)
  hm.ezg[paste0("curvature_planar_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Openness
  scale0 <- 20
  raster_name_l1 <- paste0("openness_pos_",tile_name_l1,"_",scale0)
  hm.ezg[paste0("openness_pos_",scale0,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l1 <- paste0("openness_neg_",tile_name_l1,"_",scale0)
  hm.ezg[paste0("openness_neg_",scale0,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  scale1 <- 50
  raster_name_l1 <- paste0("openness_pos_",tile_name_l1,"_",scale1)
  hm.ezg[paste0("openness_pos_",scale1,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l1 <- paste0("openness_neg_",tile_name_l1,"_",scale1)
  hm.ezg[paste0("openness_neg_",scale1,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  scale2 <- 200
  raster_name_l1 <- paste0("openness_pos_",tile_name_l1,"_",scale2)
  hm.ezg[paste0("openness_pos_",scale2,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l1 <- paste0("openness_neg_",tile_name_l1,"_",scale2)
  hm.ezg[paste0("openness_neg_",scale2,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  
  #Flow Accumulation (Top-Down) Contributing Area
  raster_name_l1 <- paste0("upstream_area_D8_",tile_name_l1)
  hm.ezg[paste0("flow_accumulation_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("upstream_area_D8_",tile_name_l2)
  hm.ezg[paste0("flow_accumulation_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("upstream_area_D8_",tile_name_l3)
  hm.ezg[paste0("flow_accumulation_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  #Elevation percentile
  scale1 <- 50
  raster_name_l1 <- paste0("elevation_percentile_",tile_name_l1,"_",scale1)
  hm.ezg[paste0("elevation_percentile_",scale1,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  scale2 <- 200
  raster_name_l1 <- paste0("elevation_percentile_",tile_name_l1,"_",scale2)
  hm.ezg[paste0("elevation_percentile_",scale2,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  
  #Topographic Position Indexes
  raster_name_l1 <- paste0("tpi_15m__",tile_name_l1)
  hm.ezg[paste0("tpi_15m_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("tpi_50m__",tile_name_l2)
  hm.ezg[paste0("tpi_50m_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l2 <- paste0("tpi_200m__",tile_name_l2)
  hm.ezg[paste0("tpi_200m_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("tpi_500m__",tile_name_l3)
  hm.ezg[paste0("tpi_500m_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("tpi_1km__",tile_name_l3)
  hm.ezg[paste0("tpi_1km_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("tpi_2km__",tile_name_l3)
  hm.ezg[paste0("tpi_2km_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("tpi_4km__",tile_name_l3)
  hm.ezg[paste0("tpi_4km_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Multiresolution Index of Valley Bottom Flatness (MRVBF)
  raster_name_l1 <- paste0("mrvbf_",tile_name_l1)
  hm.ezg[paste0("mrvbf_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("mrvbf_",tile_name_l2)
  hm.ezg[paste0("mrvbf_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("mrvbf_",tile_name_l3)
  hm.ezg[paste0("mrvbf_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # MultiresolutionIndex of the Ridge Top Flatness (MRRTF)
  raster_name_l1 <- paste0("mrrtf_",tile_name_l1)
  hm.ezg[paste0("mrrtf_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("mrrtf_",tile_name_l2)
  hm.ezg[paste0("mrrtf_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("mrrtf_",tile_name_l3)
  hm.ezg[paste0("mrrtf_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Convergence index
  raster_name_l1 <- paste0("cindex_",tile_name_l1)
  hm.ezg[paste0("convergence_index_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("cindex_",tile_name_l2)
  hm.ezg[paste0("convergence_index_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("cindex_",tile_name_l3)
  hm.ezg[paste0("convergence_index_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Upslope Curvature
  raster_name_l1 <- paste0("curvup_",tile_name_l1)
  hm.ezg[paste0("curv_upslope_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("curvup_",tile_name_l2)
  hm.ezg[paste0("curv_upslope_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("curvup_",tile_name_l3)
  hm.ezg[paste0("curv_upslope_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Downslope Curvature
  raster_name_l1 <- paste0("curvdown_",tile_name_l1)
  hm.ezg[paste0("curv_downslope_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("curvdown_",tile_name_l2)
  hm.ezg[paste0("curv_downslope_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("curvdown_",tile_name_l3)
  hm.ezg[paste0("curv_downslope_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Morphometric Protection Index
  raster_name_l1 <- paste0("mpi_r30_",tile_name_l1)
  hm.ezg[paste0("mpi_r30_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("mpi_r30_",tile_name_l2)
  hm.ezg[paste0("mpi_r30_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("mpi_r30_",tile_name_l3)
  hm.ezg[paste0("mpi_r30_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Vector Ruggedness Measure (VRM)
  raster_name_l1 <- paste0("vrm_r5_",tile_name_l1)
  hm.ezg[paste0("vrm_r5_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("vrm_r5_",tile_name_l2)
  hm.ezg[paste0("vrm_r5_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("vrm_r5_",tile_name_l3)
  hm.ezg[paste0("vrm_r5_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Terrain Ruggedness Index (TRI)
  raster_name_l1 <- paste0("tri_r5_",tile_name_l1)
  hm.ezg[paste0("tri_r5_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("tri_r5_",tile_name_l2)
  hm.ezg[paste0("tri_r5_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("tri_r5_",tile_name_l3)
  hm.ezg[paste0("tri_r5_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Slope Height
  raster_name_l1 <- paste0("heigslope_",tile_name_l1)
  hm.ezg[paste0("slope_height_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("heigslope_",tile_name_l2)
  hm.ezg[paste0("slope_height_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("heigslope_",tile_name_l3)
  hm.ezg[paste0("slope_height_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Valley Depth
  raster_name_l1 <- paste0("heigval_",tile_name_l1)
  hm.ezg[paste0("valley_depth_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("heigval_",tile_name_l2)
  hm.ezg[paste0("valley_depth_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("heigval_",tile_name_l3)
  hm.ezg[paste0("valley_depth_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Normalized Height
  raster_name_l1 <- paste0("heignor_",tile_name_l1)
  hm.ezg[paste0("normalized_height_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("heignor_",tile_name_l2)
  hm.ezg[paste0("normalized_height_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("heignor_",tile_name_l3)
  hm.ezg[paste0("normalized_height_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Standardized Height
  raster_name_l1 <- paste0("heigstand_",tile_name_l1)
  hm.ezg[paste0("standardized_height_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("heigstand_",tile_name_l2)
  hm.ezg[paste0("standardized_height_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("heigstand_",tile_name_l3)
  hm.ezg[paste0("standardized_height_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Mid-Slope Positon
  raster_name_l1 <- paste0("heigmid_",tile_name_l1)
  hm.ezg[paste0("mid_slope_pos_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("heigmid_",tile_name_l2)
  hm.ezg[paste0("mid_slope_pos_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("heigmid_",tile_name_l3)
  hm.ezg[paste0("mid_slope_pos_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Aspect Northness
  raster_name_l1 <- paste0("asp_nness_",tile_name_l1)
  hm.ezg[paste0("aspect_nness_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("asp_nness_",tile_name_l2)
  hm.ezg[paste0("aspect_nness_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("asp_nness_",tile_name_l3)
  hm.ezg[paste0("aspect_nness_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Aspect Eastness
  raster_name_l1 <- paste0("asp_eness_",tile_name_l1)
  hm.ezg[paste0("aspect_eness_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l2 <- paste0("asp_eness_",tile_name_l2)
  hm.ezg[paste0("aspect_eness_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l3 <- paste0("asp_eness_",tile_name_l3)
  hm.ezg[paste0("aspect_eness_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  # Toposcale
  t.srn <- 5
  t.srx <- 185
  t.inc <- 20
  raster_name_l1 <- paste0("toposc_",tile_name_l1, t.srn, "_", t.srx, "_", t.inc)
  hm.ezg[paste0("toposcale_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  
  #Ground Cover
  raster_name_l1 <- paste0("gc_",tile_name_l1)
  hm.ezg[paste0("gc_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  
  #Geomorphon
  scale1 <- 50
  raster_name_l1 <- paste0("geomorphon_",tile_name_l1,"_",scale1)
  hm.ezg[paste0("geomorphon_",scale1,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  scale2 <- 200
  raster_name_l1 <- paste0("geomorphon_",tile_name_l1,"_",scale2)
  hm.ezg[paste0("geomorphon_",scale2,"_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  
  #Slide susceptibility

  raster_name_l1 <- paste0("unconsolidated_deposits_susceptibility_",tile_name_l1)
  hm.ezg[paste0("susceptibility_unconsolidated_deposits_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tiff")
  
  raster_name_l1 <- paste0("unconsolidated_deposits_id_",tile_name_l1)
  hm.ezg[paste0("id_unconsolidated_deposits_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tiff")
  
  raster_name_l1 <- paste0("gk500_susceptibility_",tile_name_l1)
  hm.ezg[paste0("susceptibility_gk500_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tiff")
  
  raster_name_l1 <- paste0("gk500_id_",tile_name_l1)
  hm.ezg[paste0("id_gk500_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tiff")
  
  
  #VHM
  raster_name_l1 <- paste0("vhm_max_",tile_name_l1)
  hm.ezg[paste0("vhm_max_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  raster_name_l1 <- paste0("vhm_q3_",tile_name_l1)
  hm.ezg[paste0("vhm_q3_",res_l1)] <- sample_raster(hm.ezg,tile_output_folder_l1,raster_name_l1,"tif")
  
  raster_name_l2 <- paste0("vhm_max_",tile_name_l2)
  hm.ezg[paste0("vhm_max_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  raster_name_l2 <- paste0("vhm_q3_",tile_name_l2)
  hm.ezg[paste0("vhm_q3_",res_l2)] <- sample_raster(hm.ezg,tile_output_folder_l2,raster_name_l2,"tif")
  
  raster_name_l3 <- paste0("vhm_max_",tile_name_l3)
  hm.ezg[paste0("vhm_max_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("vhm_q3_",tile_name_l3)
  hm.ezg[paste0("vhm_q3_",res_l3)] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  
  # #Precipitation
  raster_name_l3 <- paste0("prcp_60m_2a",tile_name_l3)
  hm.ezg["prcp_60m_2a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_2a_0025",tile_name_l3)
  hm.ezg["prcp_60m_2a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_2a_0975",tile_name_l3)
  hm.ezg["prcp_60m_2a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_60m_10a",tile_name_l3)
  hm.ezg["prcp_60m_10a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_10a_0025",tile_name_l3)
  hm.ezg["prcp_60m_10a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_10a_0975",tile_name_l3)
  hm.ezg["prcp_60m_10a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_60m_30a",tile_name_l3)
  hm.ezg["prcp_60m_30a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_30a_0025",tile_name_l3)
  hm.ezg["prcp_60m_30a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_30a_0975",tile_name_l3)
  hm.ezg["prcp_60m_30a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_60m_50a",tile_name_l3)
  hm.ezg["prcp_60m_50a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_50a_0025",tile_name_l3)
  hm.ezg["prcp_60m_50a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_50a_0975",tile_name_l3)
  hm.ezg["prcp_60m_50a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_60m_100a",tile_name_l3)
  hm.ezg["prcp_60m_100a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_100a_0025",tile_name_l3)
  hm.ezg["prcp_60m_100a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_100a_0975",tile_name_l3)
  hm.ezg["prcp_60m_100a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_60m_200a",tile_name_l3)
  hm.ezg["prcp_60m_200a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_200a_0025",tile_name_l3)
  hm.ezg["prcp_60m_200a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_200a_0975",tile_name_l3)
  hm.ezg["prcp_60m_200a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_60m_300a",tile_name_l3)
  hm.ezg["prcp_60m_300a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_300a_0025",tile_name_l3)
  hm.ezg["prcp_60m_300a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_60m_300a_0975",tile_name_l3)
  hm.ezg["prcp_60m_300a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_24h_2a",tile_name_l3)
  hm.ezg["prcp_24h_2a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_2a_0025",tile_name_l3)
  hm.ezg["prcp_24h_2a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_2a_0975",tile_name_l3)
  hm.ezg["prcp_24h_2a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_24h_10a",tile_name_l3)
  hm.ezg["prcp_24h_10a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_10a_0025",tile_name_l3)
  hm.ezg["prcp_24h_10a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_10a_0975",tile_name_l3)
  hm.ezg["prcp_24h_10a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_24h_30a",tile_name_l3)
  hm.ezg["prcp_24h_30a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_30a_0025",tile_name_l3)
  hm.ezg["prcp_24h_30a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_30a_0975",tile_name_l3)
  hm.ezg["prcp_24h_30a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_24h_50a",tile_name_l3)
  hm.ezg["prcp_24h_50a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_50a_0025",tile_name_l3)
  hm.ezg["prcp_24h_50a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_50a_0975",tile_name_l3)
  hm.ezg["prcp_24h_50a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_24h_100a",tile_name_l3)
  hm.ezg["prcp_24h_100a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_100a_0025",tile_name_l3)
  hm.ezg["prcp_24h_100a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_100a_0975",tile_name_l3)
  hm.ezg["prcp_24h_100a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_24h_200a",tile_name_l3)
  hm.ezg["prcp_24h_200a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_200a_0025",tile_name_l3)
  hm.ezg["prcp_24h_200a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_200a_0975",tile_name_l3)
  hm.ezg["prcp_24h_200a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  raster_name_l3 <- paste0("prcp_24h_300a",tile_name_l3)
  hm.ezg["prcp_24h_300a"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_300a_0025",tile_name_l3)
  hm.ezg["prcp_24h_300a_0025"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  raster_name_l3 <- paste0("prcp_24h_300a_0975",tile_name_l3)
  hm.ezg["prcp_24h_300a_0975"] <- sample_raster(hm.ezg,tile_output_folder_l3,raster_name_l3,"tif")
  
  
  #KoBo Soil Data
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_CECpot_depth_0_30"
  hm.ezg["soil_CECpot_depth_0_30"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_CECpot_depth_0_30_err"
  hm.ezg["soil_CECpot_depth_0_30_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_CECpot_depth_30_60"
  hm.ezg["soil_CECpot_depth_30_60"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_CECpot_depth_30_60_err"
  hm.ezg["soil_CECpot_depth_30_60_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_CECpot_depth_60_120"
  hm.ezg["soil_CECpot_depth_60_120"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_CECpot_depth_60_120_err"
  hm.ezg["soil_CECpot_depth_60_120_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_clay_depth_0_30"
  hm.ezg["soil_clay_depth_0_30"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_clay_depth_0_30_err"
  hm.ezg["soil_clay_depth_0_30_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_clay_depth_30_60"
  hm.ezg["soil_clay_depth_30_60"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_clay_depth_30_60_err"
  hm.ezg["soil_clay_depth_30_60_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_clay_depth_60_120"
  hm.ezg["soil_clay_depth_60_120"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_clay_depth_60_120_err"
  hm.ezg["soil_clay_depth_60_120_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_pH_depth_0_30"
  hm.ezg["soil_pH_depth_0_30"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_pH_depth_0_30_err"
  hm.ezg["soil_pH_depth_0_30_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_pH_depth_30_60"
  hm.ezg["soil_pH_depth_30_60"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_pH_depth_30_60_err"
  hm.ezg["soil_pH_depth_30_60_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_pH_depth_60_120"
  hm.ezg["soil_pH_depth_60_120"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_pH_depth_60_120_err"
  hm.ezg["soil_pH_depth_60_120_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_sand_depth_0_30"
  hm.ezg["soil_sand_depth_0_30"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_sand_depth_0_30_err"
  hm.ezg["soil_sand_depth_0_30_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_sand_depth_30_60"
  hm.ezg["soil_sand_depth_30_60"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_sand_depth_30_60_err"
  hm.ezg["soil_sand_depth_30_60_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_sand_depth_60_120"
  hm.ezg["soil_sand_depth_60_120"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_sand_depth_60_120_err"
  hm.ezg["soil_sand_depth_60_120_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_silt_depth_0_30"
  hm.ezg["soil_silt_depth_0_30"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_silt_depth_0_30_err"
  hm.ezg["soil_silt_depth_0_30_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_silt_depth_30_60"
  hm.ezg["soil_silt_depth_30_60"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_silt_depth_30_60_err"
  hm.ezg["soil_silt_depth_30_60_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_silt_depth_60_120"
  hm.ezg["soil_silt_depth_60_120"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_silt_depth_60_120_err"
  hm.ezg["soil_silt_depth_60_120_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_soc_depth_0_30"
  hm.ezg["soil_soc_depth_0_30"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_soc_depth_0_30_err"
  hm.ezg["soil_soc_depth_0_30_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_soc_depth_30_60"
  hm.ezg["soil_soc_depth_30_60"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_soc_depth_30_60_err"
  hm.ezg["soil_soc_depth_30_60_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_soc_depth_60_120"
  hm.ezg["soil_soc_depth_60_120"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  raster_name_l1 <- "../020_HAFL_Phd_Projekt_Rutschungen/Daten/Soil_soc_depth_60_120_err"
  hm.ezg["soil_soc_depth_60_120_err"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  
  
  #Sample HADES 2.4b
  raster_name_l1 <- "../HADES_Tafel_242/Tafel_242_1h2-33a_lv95"
  hm.ezg["hades_1h_2_33a_maximum"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"shp","Maximum")
  hm.ezg["hades_1h_2_33a_minimum"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"shp","Minimum")
  
  raster_name_l1 <- "../HADES_Tafel_242/Tafel_242_24h2-33a_lv95"
  hm.ezg["hades_24h_2_33a_maximum"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"shp","Maximum")
  hm.ezg["hades_24h_2_33a_minimum"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"shp","Minimum")
  
  raster_name_l1 <- "../HADES_Tafel_242/Tafel_242_1h100a_lv95"
  hm.ezg["hades_1h_100a_maximum"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"shp","Maximum")
  hm.ezg["hades_1h_100a_minimum"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"shp","Minimum")
  
  raster_name_l1 <- "../HADES_Tafel_242/Tafel_242_24h100a_lv95"
  hm.ezg["hades_24h_100a_maximum"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"shp","Maximum")
  hm.ezg["hades_24h_100a_minimum"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"shp","Minimum")
  
  
  #SWECO25 variables
  raster_name_l1 <- "../SWECO25/hydro/dist2riverstrahler/hydro_gwn07_dist2riverstrahler_all"
  hm.ezg["dist2river_25"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  
  raster_name_l1 <- "../SWECO25/hydro/dist2lake/hydro_gwn07_dist2lake_all"
  hm.ezg["dist2lake_25"] <- sample_raster(hm.ezg,getwd(),raster_name_l1,"tif")
  
  #Gesteinsdichte
  raster_name_l1 <- "./geo_rockdensity"
  hm.ezg["rockdensity_litho"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"gpkg","SAPHYR_N")
  hm.ezg["rockdensity_litho_id"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"gpkg","saphyr_id")
  hm.ezg["rhob_m"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"gpkg","RHOB_M")
  hm.ezg["rhob_med"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"gpkg","RHOB_MED")
  hm.ezg["rhob_p05"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"gpkg","RHOB_p05")
  hm.ezg["rhob_p25"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"gpkg","RHOB_p25")
  hm.ezg["rhob_p75"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"gpkg","RHOB_p75")
  hm.ezg["rhob_p95"] <- sample_vector(hm.ezg,getwd(),raster_name_l1,"gpkg","RHOB_p95")
  
  return(hm.ezg)
}


#
# Sample points for HMDB data
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

watersheds.filtered <- filter(watersheds, TEZGNR150 %in% watersheds.target)
nrow(watersheds.filtered)

#Paths and configs
output_base_path <- "./data/geomorph"

setwd(output_base_path)

n.cores <- 20
cl <- makeCluster(n.cores)
clusterEvalQ(cl, {
  library(terra)
  library(rgdal)
  library(sf)
})  # you need to export packages as well

clusterExport(cl, varlist=c("tile_name_template","tile_folder_path","tile_id_name","res_l1","hm.joined","watersheds.target","process_tile","sample_raster","sample_vector","tile_name_template","tile_name_template_l2","tile_name_template_l3","tile_folder","res_l1","res_l2","res_l3"), envir=environment())

hm.res <- parallel::parLapply(cl=cl, X=seq(length(watersheds.target)),
                              fun =
                                function(x) {
                                  tile_id = watersheds.target[x]

                                  return(process_tile(tile_id = tile_id))

                                }
)
stopCluster(cl)
warnings()

hm.res <- do.call(rbind.data.frame, hm.res)
nrow(hm.res)

slide_out_path <-"./../hangmuren-export-2023-04-10-10-06_modified.csv"
write.csv(hm.res,slide_out_path, row.names = FALSE)

# Function for sampling a file with generated points in rock signature
processRockpoints <- function(input_path, output_path){ 
  base_path <- "E:/GIS_Projekte/Paper_2"
  setwd(base_path)
  
  hm.df <-  read.csv(input_path, stringsAsFactors = FALSE, sep = ",", quote = '"', skip = 0)
  head(hm.df)
  colnames(hm.df)
  str(hm.df)
  
  
  crs.lv95 <- st_crs("EPSG:2056")
  
  hm.spdf <- st_as_sf(hm.df, coords = c("x", "y"), crs = crs.lv95)
  
  hm.spdf$X.Koordinate <- hm.df$x
  hm.spdf$Y.Koordinate <- hm.df$y
  
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
  
  n.cores <- 20
  
  cl <- makeCluster(n.cores)
  
  clusterEvalQ(cl, {
    library(terra)
    library(rgdal)
    library(sf)
  })  # you need to export packages as well
  
  clusterExport(cl, varlist=c("tile_name_template","tile_folder_path","tile_id_name","res_l1","hm.joined","watersheds.target","process_tile","sample_raster","sample_vector","tile_name_template","tile_name_template_l2","tile_name_template_l3","tile_folder","res_l1","res_l2","res_l3"), envir=environment())
  
  hm.res <- parallel::parLapply(cl=cl, X=seq(length(watersheds.target)),
                                fun =
                                  function(x) {
                                    tile_id = watersheds.target[x]
                                    
                                    return(process_tile(tile_id = tile_id))
                                    
                                  }
  )
  stopCluster(cl)
  warnings()
  
  hm.res <- do.call(rbind.data.frame, hm.res)
  nrow(hm.res)
  
  write.csv(hm.res,output_path, row.names = FALSE)
  
}

processRockpoints("./data/hmdb_sampled_slope_unrestricted_proportional.csv","./../hmdb_sampled_slope_unrestricted_proportional_modified.csv")


#
# Sample points for KtBE data
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

hm.res <- NULL
for (tile_id in watersheds.target) {
  slide_out_path <-paste0("E:/GIS_Projekte/Paper_2/data/geomorph/ch_tiles/dem_tile_5_ezg",tile_id)
  if(!file.exists(slide_out_path)) {
    print(tile_id)
  }
}

n.cores <- 20
cl <- makeCluster(n.cores)

clusterEvalQ(cl, {
  library(terra)
  library(rgdal)
  library(sf)
})  # you need to export packages as well

clusterExport(cl, varlist=c("tile_name_template","tile_folder_path","tile_id_name","res_l1","hm.joined","watersheds.target","process_tile","sample_raster","sample_vector","tile_name_template","tile_name_template_l2","tile_name_template_l3","tile_folder","res_l1","res_l2","res_l3"), envir=environment())

hm.res <- parallel::parLapply(cl=cl, X=seq(length(watersheds.target)),
                              fun = 
                                function(x) {
                                  tile_id = watersheds.target[x]
                                  
                                  return(process_tile(tile_id = tile_id))
                                  
                                }
)
stopCluster(cl)

warnings()

hm.res <- do.call(rbind.data.frame, hm.res)
nrow(hm.res)

slide_out_path <-"./../SpontRutschBE.csv"
write.csv(hm.res,slide_out_path, row.names = FALSE)

processRockpoints("./data/be_sampled_slope_unrestricted_proportional.csv","./../be_sampled_slope_unrestricted_proportional_modified.csv")


#
# Sample points for StorMe data
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

st_crs(hm.spdf)
st_crs(watersheds)

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

watersheds.filtered <- filter(watersheds, TEZGNR150 %in% watersheds.target)
nrow(watersheds.filtered)

output_base_path <- "./data/geomorph"
setwd(output_base_path)

watershed.target.filtered <- NULL

for (tile_id in watersheds.target) {
  slide_out_path <-paste0("./../storme_interlis_merged_selection_modified_redux3_",tile_id,".csv")
  if (!file.exists(slide_out_path)) {
    watershed.target.filtered <- rbind(watershed.target.filtered,tile_id)
  }  
}
watershed.target.filtered

length(watersheds.target)
length(watershed.target.filtered)



n.cores <- 20
cl <- makeCluster(n.cores)

clusterEvalQ(cl, {
  library(terra)
  library(rgdal)
  library(sf)
})  # you need to export packages as well

clusterExport(cl, varlist=c("tile_name_template","tile_folder_path","tile_id_name","res_l1","hm.joined","watersheds.target", "watershed.target.filtered","process_tile","sample_raster","sample_vector","tile_name_template","tile_name_template_l2","tile_name_template_l3","tile_folder","res_l1","res_l2","res_l3"), envir=environment())


id.res <- parallel::parLapply(cl=cl, X=seq(length(watershed.target.filtered)),
                              fun = 
                                function(x) {
                                  tile_id = watershed.target.filtered[x]
                                  
                                  slide_out_path <-paste0("./../storme_interlis_merged_selection_modified_redux3_",tile_id,".csv")
                                  if (!file.exists(slide_out_path)) {
                                    hm.res <- process_tile(tile_id = tile_id)
                                    print(nrow(hm.res))
                                    
                                    write.csv(hm.res,slide_out_path, row.names = FALSE)
                                    return(tile_id)
                                  }
                                  
                                }
)

warnings()
stopCluster(cl)


filenames <- Sys.glob(paste0(getwd(),"/../storme_interlis_merged_selection_modified_redux3_*.csv"), dirmark = FALSE)
hm.res <- NULL
for(fn in filenames) {
  csv.df <- read.csv(fn,sep=",",quot='"',stringsAsFactors = FALSE,fileEncoding="latin1", row.names=NULL)
  hm.res <- rbind(hm.res,csv.df)
}
slide_out_path <-paste0("./../storme_interlis_merged_selection_modified_redux3.csv")
write.csv(hm.res,slide_out_path, row.names = FALSE)

processRockpoints("./data/storme_sampled_slope_unrestricted_proportional.csv","./../storme_sampled_slope_unrestricted_proportional_modified.csv")
