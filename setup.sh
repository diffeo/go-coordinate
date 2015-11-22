#!/bin/sh
#
# setup.sh: prepare a Docker build environment
#
# This copies content from the go-cordinate source directory to the current
# directory, so that you can run "docker build" to get a container out.
# Typical use is:
#
#   mkdir docker-build
#   cd docker-build
#   ../setup.sh
#   docker build -t $USER/coordinated:$(cat container-version) .
#
# If a command-line argument is given to this script, it is taken as a
# build number and appended to the git-derived container version.

# Break automatically on any command error
set -e

# Directory containing this script
D=$(cd $(dirname "$0") && pwd -P)

# Output directory
O=$(pwd -P)

# Build number suffix (if any)
B=${1:+-$1}

# If this is itself running inside a Docker container, set an
# environment variable HOST_MAP to /host/path=/container/path, so the
# "docker run -v" option can be set correctly.
if [ -n "$HOST_MAP" ]; then
    SED_EXPR=$(echo "$HOST_MAP" | sed "s@\(.*\)=\(.*\)@s=^\2=\1=@")
    HOST_GOPATH=$(echo "$GOPATH" | sed "$SED_EXPR")
    HOST_D=$(echo "$D" | sed "$SED_EXPR")
    HOST_O=$(echo "$O" | sed "$SED_EXPR")
else
    HOST_GOPATH="$GOPATH"
    HOST_D="$D"
    HOST_O="$O"
fi

# Pre-build a static binary
docker run --rm \
       -v "$HOST_GOPATH":/gopath \
       -v "$HOST_D":/usr/src/go-coordinate \
       -v "$HOST_O":/usr/src/output \
       -e GOPATH=/gopath \
       -e CGO_ENABLED=0 \
       -w /usr/src/go-coordinate \
       golang:1.5.1 \
       go build -a --installsuffix cgo --ldflags=-s -o /usr/src/output/coordinated.bin ./cmd/coordinated

# Create the version stamp file
V=$(cd "$D" && git describe HEAD)
echo "$V$B" > "$O/container-version"

# Copy in any additional files required
if [ "$D" != "$O" ]; then
    cp -a "$D/Dockerfile" "$O"
fi
