# Dockerfile for building coordinated into a container.
# setup.sh will prepare prerequisites in the current directory.
FROM scratch
MAINTAINER Diffeo Support <support@diffeo.com>

COPY coordinated.bin /coordinated
ENTRYPOINT ["/coordinated"]
