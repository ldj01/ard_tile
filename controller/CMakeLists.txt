cmake_minimum_required ( VERSION 2.8 )


find_package ( PythonInterp 2.7.0 REQUIRED )
find_package ( PythonLibs 2.7.0 REQUIRED )


install (FILES ard_tile.json
         DESTINATION ${PROJECT_INSTALL_PATH})


InstallExecutable (ardtile.py)
