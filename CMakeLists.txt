cmake_minimum_required ( VERSION 2.8 )


include ( InstallExecutable.cmake )


set ( PROJECT_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}/usgs/ardtile )
set ( LINK_PROJECT_EXECUTABLES true )


project ( ardtile )


set ( SYSTEM_NAME ardtile )
set ( SYSTEM_VERSION 0.6.1 )


add_subdirectory ( controller )
