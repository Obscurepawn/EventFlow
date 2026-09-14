ARG UBUNTU_IMAGE=ubuntu:24.04@sha256:224a1869083a311ef3f13648a154ba79832fbef6364d31493642ca03082da254
FROM ${UBUNTU_IMAGE} AS toolchain

RUN test "$(dpkg --print-architecture)" = amd64 \
    && apt-get -o Acquire::Retries=3 update \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=3 install -y --no-install-recommends ca-certificates \
    && sed -i 's|http://|https://|g' /etc/apt/sources.list.d/ubuntu.sources \
    && apt-get -o Acquire::Retries=3 update \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=3 install -y --no-install-recommends \
       bash binutils ca-certificates clang-18 cmake g++-13 \
       libclang-rt-18-dev lld-18 make tini \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/clang-18 /usr/local/bin/clang \
    && ln -s /usr/bin/clang++-18 /usr/local/bin/clang++ \
    && ln -s /usr/bin/ld.lld-18 /usr/local/bin/ld.lld \
    && groupadd --gid 10001 reviewer \
    && useradd --uid 10001 --gid 10001 --create-home --shell /bin/bash reviewer \
    && install -d -o 10001 -g 10001 /opt/eventflow

# Clang ThinLTO needs LLVM archivers for the project's static libraries.
RUN apt-get -o Acquire::Retries=3 update \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=3 install -y --no-install-recommends llvm-18 \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/llvm-ar-18 /usr/local/bin/llvm-ar \
    && ln -s /usr/bin/llvm-ranlib-18 /usr/local/bin/llvm-ranlib

COPY docker/entrypoint.sh /usr/local/bin/eventflow-review
RUN chmod 0755 /usr/local/bin/eventflow-review
ENV EVENTFLOW_SOURCE_DIR=/opt/eventflow
WORKDIR /opt/eventflow
USER 10001:10001
COPY --chown=10001:10001 cpp3rdlib/ cpp3rdlib/
RUN cd cpp3rdlib && sha256sum --check --quiet MANIFEST.sha256
COPY --chown=10001:10001 docker/README.md docker/README.md
RUN { clang++ --version; cmake --version; getconf GNU_LIBC_VERSION; \
      dpkg-query -W clang-18 cmake g++-13 libstdc++6 libc6 libclang-rt-18-dev lld-18 llvm-18; } \
    > TOOLCHAIN-VERSIONS.txt
LABEL org.opencontainers.image.title="EventFlow build environment" \
      org.opencontainers.image.description="Clang C++20 toolchain and bundled third-party dependencies"
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/usr/local/bin/eventflow-review"]
CMD ["help"]

FROM toolchain AS reviewer
COPY --chown=10001:10001 CMakeLists.txt Makefile .clang-format ./
COPY --chown=10001:10001 cmake/ cmake/
COPY --chown=10001:10001 include/ include/
COPY --chown=10001:10001 src/ src/
COPY --chown=10001:10001 tests/ tests/
COPY --chown=10001:10001 examples/ examples/

COPY --chown=10001:10001 README.md Dockerfile .dockerignore ./
RUN { printf '%s\n' CMakeLists.txt Makefile .clang-format README.md Dockerfile .dockerignore docker/README.md /usr/local/bin/eventflow-review; \
      find cmake cpp3rdlib include src tests examples -type f -o -type l; } \
    | LC_ALL=C sort | xargs sha256sum > SOURCE_MANIFEST.sha256 \
    && { clang++ --version; cmake --version; getconf GNU_LIBC_VERSION; \
         dpkg-query -W clang-18 cmake g++-13 libstdc++6 libc6 libclang-rt-18-dev lld-18 llvm-18; } \
       > TOOLCHAIN-VERSIONS.txt

LABEL org.opencontainers.image.title="EventFlow reviewer environment" \
      org.opencontainers.image.description="C++20 source, Clang toolchain and bundled dependencies" \
      org.opencontainers.image.version="0.1.0"
CMD ["help"]
