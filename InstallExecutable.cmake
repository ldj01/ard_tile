##########################
# Install an executable as a symbolic link

macro(InstallExecutable _source_dir _name)

    if (NOT DEFINED PROJECT_INSTALL_PATH OR
        (DEFINED PROJECT_INSTALL_PATH AND (PROJECT_INSTALL_PATH STREQUAL "")))
        message ("Please define PROJECT_INSTALL_PATH")

    else ()

        set (_bin_path ${PROJECT_INSTALL_PATH}/bin)

        # Install the executable
        install(PROGRAMS ${_source_dir}/${_name}
                DESTINATION ${_bin_path})

        if (LINK_PROJECT_EXECUTABLES)
            # Install a symbolic link to the executable
            install (
                CODE "execute_process (
                         COMMAND mkdir -p ${CMAKE_INSTALL_PREFIX}/bin
                     )"
                CODE "execute_process (
                         COMMAND ln -sf ${_bin_path}/${_name}
                                        ${CMAKE_INSTALL_PREFIX}/bin/${_name}
                     )"
            )
        endif ()

    endif ()

endmacro(InstallExecutable)
