cmake_minimum_required(VERSION 2.8)

file(GLOB SCHEMAS "ard_metadata_v*.xsd")

install(FILES ${SCHEMAS} DESTINATION schemas)
