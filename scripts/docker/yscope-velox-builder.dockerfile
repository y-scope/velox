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
FROM ghcr.io/y-scope/docker-github-actions-runner:ubuntu-jammy

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# First, copy the dependency installation scripts to optimize Docker layer caching
COPY scripts /tmp/velox-deps/

# Set timezone to avoid interactive prompts during apt installations
ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive

# Install CMake 3.28.3 using official binary installer
# NOTE: setup-ubuntu.sh installs cmake via pip, but pip-installed cmake is not reliably found
# in container environments, causing FastFloat library build failures. Using the official
# binary installer ensures cmake is available and dependencies build correctly in the current
# CI containerized environment.
RUN wget --progress=dot:giga https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.sh \
   && chmod +x cmake-3.28.3-linux-x86_64.sh \
   && ./cmake-3.28.3-linux-x86_64.sh --skip-license --prefix=/usr/local \
   && rm cmake-3.28.3-linux-x86_64.sh

# Run the setup script to install all dependencies, then move venv to /opt and clean up
# Note: setup-ubuntu.sh creates venv at SCRIPTDIR/../.venv (i.e., /tmp/.venv)
RUN /tmp/velox-deps/setup-ubuntu.sh \
 && mv /tmp/.venv /opt/velox-venv \
 && rm -rf /tmp/velox-deps

# Activate the virtual environment
ENV PATH="/opt/velox-venv/bin:${PATH}"
ENV VIRTUAL_ENV="/opt/velox-venv"

# Configure ccache settings
# CCACHE_BASEDIR: Must be set at runtime to your source checkout path for cache hits.
#                 Example: export CCACHE_BASEDIR=$GITHUB_WORKSPACE (CI) or /path/to/velox (local)
# CCACHE_NOHASHDIR: Ignores working directory in cache keys - only file content matters.
# CCACHE_COMPRESSLEVEL: Disabled (0) for faster CI execution - trading disk space for speed.
# CCACHE_MAX_SIZE: 5GB quota (actual usage typically a few hundred MB).
ENV CCACHE_DIR=/var/cache/ccache
ENV CCACHE_COMPRESSLEVEL=0
ENV CCACHE_MAX_SIZE=5G
ENV CCACHE_NOHASHDIR=true

# Copy velox source for warmup build to populate ccache
COPY . /tmp/velox-src/
WORKDIR /tmp/velox-src

# Build velox once to warm up ccache
# NOTE:
# - We set `CCACHE_BASEDIR` so cache keys use relative paths.
# - We clear the stats after warmup so that CI builds only show their own cache hits.
COPY . /tmp/velox-src/
WORKDIR /tmp/velox-src
RUN source /opt/velox-venv/bin/activate \
    && CCACHE_BASEDIR=/tmp/velox-src make release \
    && echo "CCache statistics after warmup build:" \
    && ccache --verbose --show-stats \
    && ccache --zero-stats
RUN rm -rf /tmp/velox-src

WORKDIR /
