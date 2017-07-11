cmake_minimum_required ( VERSION 2.8 )


include ( InstallExecutable.cmake )


set ( PROJECT_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}/usgs/ard_tile )
set ( LINK_PROJECT_EXECUTABLES true )


project ( ardtile )


set ( SYSTEM_NAME ardtile )
set ( SYSTEM_VERSION 2.0.0 )


add_subdirectory ( controller )
