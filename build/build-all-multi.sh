#!/usr/bin/env bash
set -ex

PLATFORMS=${PLATFORMS:-"linux/amd64,linux/arm64"}
IMAGE=${IMAGE:-"alibaba-cloud-csi-driver:latest"}
PUSH=${PUSH:-"false"}

BUILD_ARGS=(
    --frontend dockerfile.v0
    --local context=.
    --local dockerfile=build/multi
    --opt filename=Dockerfile.multi
    --opt "platform=${PLATFORMS}"
    --opt "build-arg:CSI_VERSION=$(git describe --tags --match='v*' --always --dirty)"
    --opt "build-arg:SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct)"
)
BUILD_ARGS+=("$@")

buildctl build "${BUILD_ARGS[@]}" \
    --output "type=image,push=$PUSH,name=${IMAGE}"
buildctl build "${BUILD_ARGS[@]}" \
    --opt target=csi-controller \
    --output "type=image,push=$PUSH,name=${IMAGE}-controller"
buildctl build "${BUILD_ARGS[@]}" \
    --opt target=init \
    --output "type=image,push=$PUSH,name=${IMAGE}-init"
