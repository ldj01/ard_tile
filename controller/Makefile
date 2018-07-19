#-----------------------------------------------------------------------------
# Simple makefile for building and pushing ARD Tile Docker Images.
#-----------------------------------------------------------------------------

.PHONY: build tag login push deploy debug clean

# Inherit from upper-level make.config
TOP = ..
include $(TOP)/make.config

.DEFAULT_GOAL := build
VERSION    := $(shell cat version.txt)
REPO       := $(docker_registry)"/ard-controller"
BRANCH     := $(shell git rev-parse --abbrev-ref HEAD | tr / -)
COMMIT     := $(shell git rev-parse HEAD)
COMMIT_TAG := $(REPO):$(COMMIT)
BRANCH_TAG := $(REPO):$(BRANCH)-$(VERSION)

build:
	@docker build --target ard-controller -f Dockerfile -t $(COMMIT_TAG) --rm=true --compress $(PWD)

tag:
	@docker tag $(COMMIT_TAG) $(BRANCH_TAG)
	@$(shell [ $(BRANCH) == master ] && docker tag $(COMMIT_TAG) $(REPO):latest)

login:
	@$(if $(and $(DOCKER_USER), $(DOCKER_PASS)), docker login $(docker_registry) -u $(DOCKER_USER) -p $(DOCKER_PASS), docker login $(docker_registry))

push: login
	docker push $(REPO)

debug:
	@echo "VERSION:    $(VERSION)"
	@echo "REPO:       $(REPO)"
	@echo "BRANCH:     $(BRANCH)"
	@echo "COMMIT_TAG: $(COMMIT_TAG)"
	@echo "BRANCH_TAG: $(BRANCH_TAG)"

deploy: debug build tag push

clean:
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -delete
	@find . -name '.pytest_cache' -exec rm -rv {} +
