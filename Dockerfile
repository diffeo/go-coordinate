# Dockerfile for building goordinated into a container.
# setup.sh will prepare prerequisites in the current directory.
FROM scratch
MAINTAINER David Maze <dmaze@diffeo.com>

COPY goordinated.bin /goordinated
COPY container-version /etc/container-version
ENTRYPOINT ["/goordinated"]
