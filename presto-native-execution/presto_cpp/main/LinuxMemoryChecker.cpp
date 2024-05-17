/*
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

#include "presto_cpp/main/LinuxMemoryChecker.h"
#ifdef __linux__
#include <boost/regex.hpp>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/gen/Base.h>
#include <folly/gen/File.h>
#include <folly/gen/String.h>

#include <folly/Conv.h>
#include <folly/CppAttributes.h>
#include <folly/Format.h>
#include <folly/Range.h>
#include <folly/String.h>

#include <unistd.h> 

#endif

namespace facebook::presto {

int64_t LinuxMemoryChecker::systemUsedMemoryBytes() {
#ifdef __linux__
  size_t rssMem = 0;
  std::string smapFile = "/proc/" + std::to_string(getpid()) + "/smaps_rollup";

  static const boost::regex rssRegex(R"!(Rss:\s*(\d+)\s*kB)!");
  folly::gen::byLine(smapFile) | [&](folly::StringPiece line) -> void {
    if (boost::regex_match(
            line.begin(), line.end(), match, rssRegex)) {
      folly::StringPiece numStr(
          line.begin() + match.position(1), size_t(match.length(1)));
      rssMem = folly::to<size_t>(numStr) * 1024;
    }
  };
  return rssMem; 
#else
  return 0;
#endif
}
} // namespace facebook::presto
