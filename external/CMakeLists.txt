cmake_minimum_required(VERSION 2.8)

set (docker_registry losrlcmp02.cr.usgs.gov:5000)
execute_process(COMMAND cat ${CMAKE_CURRENT_SOURCE_DIR}/version.txt
                OUTPUT_VARIABLE VERSION
                OUTPUT_STRIP_TRAILING_WHITESPACE)
set (REPO ${docker_registry}"/geos-ard-external")
execute_process(COMMAND git rev-parse --abbrev-ref HEAD
                COMMAND tr / -
                OUTPUT_VARIABLE BRANCH
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND git rev-parse --short HEAD OUTPUT_VARIABLE COMMIT
                OUTPUT_STRIP_TRAILING_WHITESPACE)
set (COMMIT_TAG ${REPO}:${COMMIT})
set (BRANCH_TAG ${REPO}:${BRANCH}-${VERSION})

add_custom_target(external
                  COMMAND docker build --target geos-ard-external
                                       -f Dockerfile -t ${COMMIT_TAG}
                                       --rm --compress .
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_target(external-tag
                  COMMAND docker tag ${COMMIT_TAG} ${BRANCH_TAG}
                  COMMAND docker tag ${COMMIT_TAG} ${REPO}:latest
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                  DEPENDS external)
add_dependencies(tag external-tag)

add_custom_target(external-push
                  COMMAND docker push ${REPO}
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                  DEPENDS external-tag)
add_dependencies(push external-push)

add_custom_target(external-debug
                  COMMAND echo "VERSION:    ${VERSION}"
                  COMMAND echo "REPO:       ${REPO}"
                  COMMAND echo "BRANCH:     ${BRANCH}"
                  COMMAND echo "COMMIT_TAG: ${COMMIT_TAG}"
                  COMMAND echo "BRANCH_TAG: ${BRANCH_TAG}")
add_dependencies(debug external-debug)

add_custom_target(external-deploy)
add_dependencies(external-deploy external-debug external external-tag
                 external-push)
