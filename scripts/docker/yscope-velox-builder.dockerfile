# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builder image with dependencies and pre-populated ccache for faster CI builds
FROM ghcr.io/myoung34/docker-github-actions-runner:ubuntu-jammy

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Set timezone to avoid interactive prompts during apt installations
ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive

# Install CMake 3.28.3 using its install script
# NOTE: `scripts/setup-ubuntu.sh` installs CMake via pip, but sometimes the pip-installed CMake
# doesn't show up on the path in container environments (causing, for example, FastFloat library
# build failures). Using CMake's install script avoids this issue.
RUN curl --fail --location --show-error --silent --remote-name \
        https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.sh \
    && chmod +x cmake-3.28.3-linux-x86_64.sh \
    && ./cmake-3.28.3-linux-x86_64.sh --skip-license --prefix=/usr/local \
    && rm cmake-3.28.3-linux-x86_64.sh

# Copy dependency installation scripts (after rarely-changing layers for better caching)
COPY scripts /tmp/velox-deps/

RUN /tmp/velox-deps/setup-ubuntu.sh \
    && mv /tmp/.venv /opt/velox-venv \
    && rm -rf /tmp/velox-deps

# Activate the virtual environment.
#
# NOTE: We set `ENV` variables directly rather than using `source /opt/velox-venv/bin/activate`
# in a RUN command since activation in a RUN command only persists for that single instruction
# (each RUN starts a fresh shell). By setting PATH and VIRTUAL_ENV via ENV, these values are
# baked into the Docker image and persist across all subsequent RUN commands and any containers
# started from the final image.
ENV VIRTUAL_ENV="/opt/velox-venv"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

ENV CCACHE_DIR=/var/cache/ccache

# Disable compression to trade disk space for speed in CI builds
ENV CCACHE_COMPRESSLEVEL=0
ENV CCACHE_MAXSIZE=5G
# Ignore working directory in cache keys so warmup cache (built in /tmp/velox-src/) is reused
# by CI builds (which run in /__w/velox/velox/)
ENV CCACHE_NOHASHDIR=true

# Build velox once to warm up ccache
# NOTE:
# - We set `CCACHE_BASEDIR` so cache keys use relative paths.
# - We clear the stats after warmup so that CI builds only show their own cache hits.
COPY . /tmp/velox-src/
WORKDIR /tmp/velox-src
RUN CCACHE_BASEDIR=/tmp/velox-src make release \
    && echo "CCache statistics after warmup build:" \
    && ccache --verbose --show-stats \
    && ccache --zero-stats
RUN rm -rf /tmp/velox-src

WORKDIR /
