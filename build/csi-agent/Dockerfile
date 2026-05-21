FROM --platform=$BUILDPLATFORM registry-cn-hangzhou.ack.aliyuncs.com/dev/golang:1.26.3 AS builder
WORKDIR /src
ARG TARGETARCH
ARG TARGETOS
RUN --mount=type=bind,target=. \
    export GOOS=$TARGETOS && \
    export GOARCH=$TARGETARCH && \
    export CGO_ENABLED=0 && \
    go build -trimpath \
        -ldflags "-X github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/version.VERSION=${CSI_VERSION}" \
        -o /out/csi-agent ./cmd/csi-agent && \
    go build -trimpath -o /out/csi-mount-proxy-server ./cmd/mount-proxy-server


FROM --platform=$BUILDPLATFORM registry-cn-hangzhou.ack.aliyuncs.com/dev/alinux:3-update AS rpm-builder
RUN yum install -y rpm-build
RUN --mount=type=bind,source=build/csi-agent/python3-provider.spec,target=/tmp/python3-provider.spec \
    rpmbuild -bb /tmp/python3-provider.spec


FROM registry-cn-hangzhou.ack.aliyuncs.com/dev/alinux:3-update

ARG TARGETPLATFORM
# The versions specified here are for RPM packages, which may not necessarily align with the image version
ARG OSSFS_VERSION=v1.91.9.ack.1
ARG OSSFS2_VERSION=v2.0.5.ack.1
ARG ALINAS_UTILS_VERSION=1.9-0.20260407144027.3d3855.al8
ARG EFC_VERSION=1.9-20250822095129.f34347.release
# v2.1 is exclusive to a specific storage cluster version and the first to support ARM arch.
# It's temporarily unavailable for all ACK clusters,
# catering only to ARM for compatibility. An update to all CPFS storage cluster versions is required.
ARG EFC_ARM_VERSION=2.1-20260318190316.34c388.release
ARG ALINAS_UTILS_ARM_VERSION=2.1-0.20260316154701.b0a52a.generic
RUN --mount=type=bind,from=rpm-builder,source=/root/rpmbuild/RPMS/noarch,target=/tmp/rpms \
    echo 'install_weak_deps=False' >> /etc/yum.conf; \
    set -ex; \
    case "${TARGETPLATFORM}" in \
        linux/amd64) ARCH="x86_64" ;; \
        linux/arm64) ARCH="aarch_64" ;; \
        *) echo "unknown platform"; exit 1 ;; \
    esac; \
    yum install -y /tmp/rpms/python3-provider-*.rpm fuse-devel util-linux mailcap procps tini; \
    yum install -y https://ack-csiplugin.oss-cn-hangzhou.aliyuncs.com/ossfs/ossfs_${OSSFS_VERSION}_centos8.0_${ARCH}.rpm; \
    yum install -y https://ack-csiplugin.oss-cn-hangzhou.aliyuncs.com/ossfs2/ossfs2_${OSSFS2_VERSION}_centos8.0_${ARCH}.rpm; \
    if [ "${ARCH}" = "x86_64" ]; then \
        yum install -y https://aliyun-alinas-eac.oss-cn-beijing.aliyuncs.com/aliyun-alinas-utils-${ALINAS_UTILS_VERSION}.noarch.rpm; \
        yum install -y https://aliyun-alinas-eac.oss-cn-beijing.aliyuncs.com/alinas-efc-${EFC_VERSION}.${ARCH}.rpm; \
    elif [ "${ARCH}" = "aarch_64" ]; then \
        yum install -y https://aliyun-alinas-eac-cn-wulanchabu.oss-cn-wulanchabu.aliyuncs.com/efc-release/2.1/aliyun-alinas-utils-${ALINAS_UTILS_ARM_VERSION}.aarch64.rpm; \
        yum install -y https://aliyun-alinas-eac-cn-wulanchabu.oss-cn-wulanchabu.aliyuncs.com/efc-release/2.1/alinas-efc-${EFC_ARM_VERSION}.aarch64.rpm; \
    fi; \
    cp -r /etc/aliyun /etc/aliyun-defaults; \
    yum clean all; \
    # python3-pyyaml is only used by nfsdclnts (NFS server diagnostic tool, irrelevant here).
    # Remove it to silence CVE scans.
    rpm -e --nodeps python3-pyyaml


COPY --link --from=builder /out/csi-mount-proxy-server /usr/local/bin/
COPY --link --from=builder /out/csi-agent /usr/local/bin/
ENTRYPOINT [ "tini", "--", "csi-mount-proxy-server", "--driver=alinas,ossfs,ossfs2" ]