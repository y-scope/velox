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

# Copy all scripts needed for dependency installation
COPY scripts /velox/scripts/

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

# Run the setup script to install all dependencies
# Run exactly as you would on a laptop - no special environment variables
RUN /velox/scripts/setup-ubuntu.sh

# Set up environment to use the Python venv created by setup script
ENV PATH="/velox/.venv/bin:${PATH}"
ENV VIRTUAL_ENV="/velox/.venv"

# Configure ccache with BASEDIR matching GitHub Actions workspace path
# CCACHE_BASEDIR: Must match the path where source will be checked out in CI (/__w/velox/velox)
#                 so that cache keys generated during warmup build match CI runtime builds
# CCACHE_NOHASHDIR: Ignores absolute directory paths in cache keys, using only relative paths
#                   from BASEDIR. Critical for cache hits across different checkouts - without
#                   this, ccache would see warmup build files vs CI checkout files as different
#                   due to file metadata/inode differences, causing cache misses
# CCACHE_COMPRESSLEVEL: Disabled (0) for faster CI execution - trading disk space for speed
# CCACHE_MAX_SIZE: 5GB quota (actual usage typically a few hundred MB)
ENV CCACHE_DIR=/velox/.ccache
ENV CCACHE_BASEDIR=/__w/velox/velox
ENV CCACHE_COMPRESSLEVEL=0
ENV CCACHE_MAX_SIZE=5G
ENV CCACHE_NOHASHDIR=true

# Copy velox source to match GitHub Actions workspace path
# This is a temporary copy used only for the warmup build to populate ccache.
# The source will be deleted after the warmup build (see below), and CI workflows
# will check out fresh source into the same path for actual builds.
COPY . /__w/velox/velox/
WORKDIR /__w/velox/velox

# Build velox once to populate ccache with compilation results
# This "warmup build" compiles all source files and stores the results in ccache.
# In CI, fresh checkouts will benefit from these cached compilation results since
# CCACHE_NOHASHDIR makes cache keys path-independent (only content matters).
RUN source /velox/.venv/bin/activate && \
    make release && \
    echo "CCache statistics after warmup build:" && \
    ccache -vs

# Remove source files but keep ccache
# The compiled binaries and source are deleted - we only need the ccache artifacts.
# CI workflows will check out fresh source code at runtime into this same directory.
RUN rm -rf /__w/velox/velox/* && echo "Removed source files, ccache preserved"
