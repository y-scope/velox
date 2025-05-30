/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/experimental/wave/exec/WaveOperator.h"

namespace facebook::velox::wave {

class Project : public WaveOperator {
 public:
  Project(
      CompileState& state,
      RowTypePtr outputType,
      std::vector<std::vector<ProgramPtr>> levels)
      : WaveOperator(state, outputType, ""), levels_(std::move(levels)) {}

  bool isStreaming() const override {
    if (!levels_.empty() && levels_[0].size() == 1 &&
        levels_[0][0]->isSource()) {
      return false;
    }
    return true;
  }

  bool isSource() const override {
    return !isStreaming();
  }

  /// True if the last  level is a sink like aggregation or partitioned output
  /// or hash build. No output operands but the output will be consumed in
  /// another pipeline.
  bool isSink() const override {
    if (levels_.empty()) {
      // Can be temporarily empty if all instructions are fused into previous
      // and this is only to designate a wrap.
      return false;
    }
    auto& last = levels_.back();
    return last.size() == 1 && last[0]->isSink();
  }

  exec::BlockingReason isBlocked(WaveStream& stream, ContinueFuture* future)
      override;

  std::vector<AdvanceResult> canAdvance(WaveStream& Stream) override;

  void schedule(WaveStream& stream, int32_t maxRows = 0) override;

  void pipelineFinished(WaveStream& stream) override;

  void finalize(CompileState& state) override;

  std::string toString() const override {
    return fmt::format("Project {}", WaveOperator::toString());
  }

  const OperandSet& syncSet() const override {
    return computedSet_;
  }

  void callUpdateStatus(
      WaveStream& stream,
      const std::vector<WaveStream*>& otherStreams,
      AdvanceResult& advance) override;

 private:
  struct ContinueLocation {
    int32_t programIdx;
    int32_t instructionIdx;
  };

  std::vector<std::vector<ProgramPtr>> levels_;
  OperandSet computedSet_;
};

} // namespace facebook::velox::wave
