cmake_minimum_required(VERSION 2.8)

include (InstallExecutable.cmake)

set (PROJECT_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}/usgs/ard_tile)
set (LINK_PROJECT_EXECUTABLES true)

project (ardtile NONE)

set (SYSTEM_NAME ardtile)
set (SYSTEM_VERSION 1.3.1)

add_custom_target(debug)
add_custom_target(tag)
add_custom_target(push)
add_custom_target(deploy)

add_subdirectory(external)
add_subdirectory(ARD_clip)
add_subdirectory(ARD_determine_segments_framework)
add_subdirectory(controller)
add_subdirectory(metadata_schema)

