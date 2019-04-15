
VERSION := $(shell git describe HEAD)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD | tr / -)
BUILD := 0

DOCKER_REPO := diffeo/coordinated
DOCKER_IMG := $(DOCKER_REPO):$(VERSION)

.PHONY: test docker docker-push-branch docker-push-latest

test:
	go test -race -v ./...

docker:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg BUILD=$(BUILD) \
		--build-arg NOW=$(shell TZ=UTC date +%Y-%m-%dT%H:%M:%SZ) \
		-t $(DOCKER_IMG) \
		.

docker-push-branch:
	# Only intended for CI
	[ ! -z "$$CI" ]
	# Push a "latest" tag to our repository
	docker tag $(DOCKER_IMG) $(DOCKER_REPO):$(BRANCH)
	docker push $(DOCKER_REPO):$(BRANCH)

docker-push-latest:
	# Only intended for CI
	[ ! -z "$$CI" ]
	# Push image to our repository
	docker push $(DOCKER_IMG)
	# Push a "latest" tag to our repository
	docker tag $(DOCKER_IMG) $(DOCKER_REPO):latest
	docker push $(DOCKER_REPO):latest
