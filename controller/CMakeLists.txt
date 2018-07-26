cmake_minimum_required( VERSION 2.8 )

find_package ( PythonInterp 2.7.0 REQUIRED )
find_package ( PythonLibs 2.7.0 REQUIRED )

InstallExecutable (${CMAKE_CURRENT_SOURCE_DIR} ardtile.py)
