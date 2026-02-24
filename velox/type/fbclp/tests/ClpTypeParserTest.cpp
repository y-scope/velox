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

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/fbclp/ClpTypeParser.h"

namespace facebook::velox::type::fbclp {

class TypeParserTest : public ::testing::Test {};

TEST_F(TypeParserTest, rowTypeWithSpecialChars) {
  ASSERT_EQ(
      *parseClpType(
          "row($dollar$sign$ bigint,-da-sh- varchar,#ha#sh# varchar, @a@t@ varchar, \\sla\\sh\\ varchar)"),
      *ROW(
          {"$dollar$sign$", "-da-sh-", "#ha#sh#", "@a@t@", "\\sla\\sh\\"},
          {BIGINT(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()}));
  ASSERT_EQ(
      *parseClpType(
          "row(\"$dollar$sign$\" bigint,\"-da-sh-\" varchar,\"#ha#sh#\" varchar,\"@a@t@\" varchar,\"\\sla\\sh\\\" varchar)"),
      *ROW(
          {"$dollar$sign$", "-da-sh-", "#ha#sh#", "@a@t@", "\\sla\\sh\\"},
          {BIGINT(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()}));
}

} // namespace facebook::velox::type::fbclp
