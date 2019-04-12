
VERSION := $(shell git describe HEAD)
BUILD := 0

DOCKER_REPO := diffeo/coordinated
DOCKER_IMG := $(DOCKER_REPO):$(VERSION)

.PHONY: test docker

test:
	go test -race -v ./...

docker:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg BUILD=$(BUILD) \
		--build-arg NOW=$(shell TZ=UTC date +%Y-%m-%dT%H:%M:%SZ) \
		-t $(DOCKER_IMG) \
		.
