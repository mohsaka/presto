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

#include <sys/stat.h>

#include <sys/sysinfo.h>
#include <sys/types.h>
#include <cinttypes>

#endif

namespace facebook::presto {

int64_t LinuxMemoryChecker::systemUsedMemoryBytes() {
#ifdef __linux__
  static int cgroupVersion = -1;
  size_t memAvailable = 0;
  size_t memTotal = 0;
  size_t cacheMemory = 0;
  size_t inactiveAnon = 0;
  size_t activeAnon = 0;
  size_t inactiveFile = 0;
  size_t activeFile = 0;
  size_t inUseMemory = 0;
  boost::cmatch match;
  std::array<char, 50> buf;
  struct sysinfo memInfo;

  // Find out what cgroup version we have
  if (cgroupVersion == -1) {
    struct stat buffer;

    if(useMeminfo_){
      cgroupVersion = 0;
    } else if ((stat("/sys/fs/cgroup/memory/memory.usage_in_bytes", &buffer) == 0)) {
      cgroupVersion = 1;
    } else if ((stat("/sys/fs/cgroup/memory.current", &buffer) == 0)) {
      cgroupVersion = 2;
    } else {
      cgroupVersion = 0;
    }
    LOG(INFO) << fmt::format("Using cgroup version {}", cgroupVersion);
  }

  // cgroups not set up or explicitly using meminfo
  if(cgroupVersion == 0) {
    sysinfo(&memInfo);
    return (memInfo.totalram - memInfo.freeram) * memInfo.mem_unit;
  } else if (cgroupVersion == 1) {
    static const boost::regex inactiveAnonRegex(R"!(total_inactive_anon\s*(\d+)\s*)!");
    static const boost::regex activeAnonRegex(R"!(total_active_anon\s*(\d+)\s*)!");
    static const boost::regex inactiveFileRegex(R"!(total_inactive_file\s*(\d+)\s*)!");
    static const boost::regex activeFileRegex(R"!(total_active_file\s*(\d+)\s*)!");
    folly::gen::byLine("/sys/fs/cgroup/memory/memory.stat") |
        [&](folly::StringPiece line) -> void {
      if (boost::regex_match(line.begin(), line.end(), match, inactiveAnonRegex)) {
        folly::StringPiece numStr(
            line.begin() + match.position(1), size_t(match.length(1)));
        inactiveAnon = folly::to<size_t>(numStr);
      }
      if (boost::regex_match(line.begin(), line.end(), match, activeAnonRegex)) {
        folly::StringPiece numStr(
            line.begin() + match.position(1), size_t(match.length(1)));
        activeAnon = folly::to<size_t>(numStr);
      }
      if (boost::regex_match(line.begin(), line.end(), match, inactiveFileRegex)) {
        folly::StringPiece numStr(
            line.begin() + match.position(1), size_t(match.length(1)));
        inactiveFile = folly::to<size_t>(numStr);
      }
      if (boost::regex_match(line.begin(), line.end(), match, activeFileRegex)) {
        folly::StringPiece numStr(
            line.begin() + match.position(1), size_t(match.length(1)));
        activeFile = folly::to<size_t>(numStr);
      }
    };
    return inactiveAnon + activeAnon + inactiveFile + activeFile;
  } else if (cgroupVersion == 2) {
    auto currentMemoryUsageFile =
        folly::File("/sys/fs/cgroup/memory.current", O_RDONLY);
    static const boost::regex cacheRegex(R"!(inactive_file\s*(\d+)\s*)!");
    if (folly::readNoInt(currentMemoryUsageFile.fd(), buf.data(), buf.size())) {
      if (sscanf(buf.data(), "%" SCNu64, &inUseMemory) == 1) {
        // Get total cached memory from memory.stat and subtract from
        // inUseMemory
        folly::gen::byLine("/sys/fs/cgroup/memory.stat") |
            [&](folly::StringPiece line) -> void {
          if (boost::regex_match(line.begin(), line.end(), match, cacheRegex)) {
            folly::StringPiece numStr(
                line.begin() + match.position(1), size_t(match.length(1)));
            cacheMemory = folly::to<size_t>(numStr);
          }
        };
        return inUseMemory - cacheMemory;
      }
    }
  }
  return 0;

#else
  return 0;
#endif
}
} // namespace facebook::presto
