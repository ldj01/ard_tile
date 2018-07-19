#-----------------------------------------------------------------------------
# Simple makefile for building and pushing ARD Tile Docker Images.
#-----------------------------------------------------------------------------

.PHONY: build deploy debug

include make.config

.DEFAULT_GOAL := build
REPO       := $()"/geos-ard-external"

build:
	@cd controller && make build
	@cd external && make build
	@cd ARD_determine_segments_framework && make build
	@cd ARD_clip && make build

deploy:
	@cd ARD_determine_segments_framework && make deploy
	@cd ARD_clip && make deploy

debug:
	@echo "REGISTRY:       $(docker_registry)"
