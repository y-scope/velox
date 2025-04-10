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
add_executable(velox_vector_hash_all_benchmark SimpleVectorHashAllBenchmark.cpp)

target_link_libraries(
  velox_vector_hash_all_benchmark
  velox_vector
  Folly::folly
  Folly::follybenchmark
  fmt::fmt)
# This is a workaround for the use of VectorTestUtils.h which include gtest.h
target_link_libraries(
  velox_vector_hash_all_benchmark GTest::gtest)

add_executable(velox_vector_selectivity_vector_benchmark
               SelectivityVectorBenchmark.cpp)

target_link_libraries(
  velox_vector_selectivity_vector_benchmark velox_vector Folly::folly
  Folly::follybenchmark)

add_executable(copy_benchmark CopyBenchmark.cpp)

target_link_libraries(
  copy_benchmark velox_vector_test_lib Folly::folly Folly::follybenchmark)

add_executable(velox_vector_map_update_benchmark MapUpdateBenchmark.cpp)
target_link_libraries(
  velox_vector_map_update_benchmark
  velox_vector_test_lib
  Folly::folly
  Folly::follybenchmark
  gflags::gflags
  glog::glog)
