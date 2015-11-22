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

GO_MODULE="github.com/diffeo/go-coordinate"
GO_SUBMODULE="cmd/coordinated"

# Break automatically on any command error
set -e

# Directory containing this script
D=$(cd $(dirname "$0") && pwd -P)

# Output directory
O=$(pwd -P)

# Build number suffix (if any)
B=${1:+-$1}

# Create a Go environment (if needed)
if [ -z "$GOPATH" ]; then
    export GOPATH="$O/go"
    mkdir -p "$GOPATH/src/$GO_MODULE"
    (cd "$D" && (find . -name '*.go' -print0; find .git -type f -print0) | xargs -0 tar cf "$O/go.tar")
    (cd "$GOPATH/src/$GO_MODULE" && tar xf "$O/go.tar")
    (cd "$GOPATH/src/$GO_MODULE" && go get -u "./$GO_SUBMODULE")
fi

# Pre-build a static binary
(cd "$GOPATH/src/$GO_MODULE" && \
 go build -a --ldflags=-s -o "$O/coordinated.bin" "./$GO_SUBMODULE")

# Create the version stamp file
V=$(cd "$D" && git describe HEAD)
echo "$V$B" > "$O/container-version"

# Copy in any additional files required
if [ "$D" != "$O" ]; then
    cp -a "$D/Dockerfile" "$O"
fi
