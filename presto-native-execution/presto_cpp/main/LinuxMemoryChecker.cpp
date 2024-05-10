/**
* Memory calculations from 
* https://docs.docker.com/reference/cli/docker/container/stats/#:~:text=On%20Linux%2C%20the%20Docker%20CLI,use%20the%20data%20as%20needed.
*
* On Linux, the Docker CLI reports memory usage by subtracting cache usage from the total memory usage.
* The API does not perform such a calculation but rather provides the total memory usage and the amount
* from the cache so that clients can use the data as needed. The cache usage is defined as the value of
* total_inactive_file field in the memory.stat file on cgroup v1 hosts.
*
* On Docker 19.03 and older, the cache usage was defined as the value of cache field.
* On cgroup v2 hosts, the cache usage is defined as the value of inactive_file field.
**/

#include "presto_cpp/main/LinuxMemoryChecker.h"
#ifdef __linux__
#include <boost/regex.hpp>
#include <folly/gen/Base.h>
#include <folly/gen/File.h>
#include <folly/gen/String.h>
#include <folly/File.h>
#include <folly/FileUtil.h>

#include <folly/Conv.h>
#include <folly/CppAttributes.h>
#include <folly/Format.h>
#include <folly/Range.h>
#include <folly/String.h>
#endif

namespace facebook::presto {
int64_t LinuxMemoryChecker::systemUsedMemoryBytes() {
#ifdef __linux__
  size_t memAvailable = 0;
  size_t memTotal = 0;
  size_t inUseMemory = 0;
  boost::cmatch match;

  // V1 variables
  auto currentMemoryUsageV1File =
      folly::File("/sys/fs/cgroup/memory/memory.usage_in_bytes", O_RDONLY);
  static const boost::regex cacheRegexV1(R"!(total_inactive_file\s*(\d+)\s*)!");

  // V2 variables
  auto currentMemoryUsageV2File =
      folly::File("/sys/fs/cgroup/memory.current", O_RDONLY);
  static const boost::regex cacheRegexV2(R"!(inactive_file\s*(\d+)\s*)!");

  // Default case variables
  static const boost::regex memAvailableRegex(R"!(MemAvailable:\s*(\d+)\s*kB)!");
  static const boost::regex memTotalRegex(R"!(MemTotal:\s*(\d+)\s*kB)!");

  // If we are using cgroup V1
  std::array<char, 50> bufV1;

  // Read current in use memory from memory.usage_in_bytes
  if (folly::readNoInt(
          currentMemoryUsageV1File.fd(), bufV1.data(), bufV1.size())) {
    if (sscanf(bufV1.data(), "%" SCNu64, &inUseMemory) == 1) {

      // Get total cached memory from memory.stat and subtract from inUseMemory
      folly::gen::byLine("/sys/fs/cgroup/memory/memory.stat") | [&](folly::StringPiece line) -> void {
        if (boost::regex_match(line.begin(), line.end(), match, cacheRegexV1)) {
          folly::StringPiece numStr(
            line.begin() + match.position(1), size_t(match.length(1)));
          return inUseMemory - folly::to<size_t>(numStr);
        }
      };

    }
  }

  // If we are using cgroup V2
  std::array<char, 50> bufV2;
  inUseMemory = 0;
  if (folly::readNoInt(
          currentMemoryUsageV2File.fd(), bufV2.data(), bufV2.size())) {
    if (sscanf(bufV2.data(), "%" SCNu64, &inUseMemory) == 1) {

      // Get total cached memory from memory.stat and subtract from inUseMemory
      folly::gen::byLine("/sys/fs/cgroup/memory.stat") | [&](folly::StringPiece line) -> void {
        if (boost::regex_match(line.begin(), line.end(), match, cacheRegexV2)) {
          folly::StringPiece numStr(
            line.begin() + match.position(1), size_t(match.length(1)));
          return inUseMemory - folly::to<size_t>(numStr);
        }
      };

    }
  }

  // Last resort use host machine info
  folly::gen::byLine("/proc/meminfo") | [&](folly::StringPiece line) -> void {
    if (boost::regex_match(line.begin(), line.end(), match, memAvailableRegex)) {
      folly::StringPiece numStr(
          line.begin() + match.position(1), size_t(match.length(1)));
      memAvailable = folly::to<size_t>(numStr) * 1024;
    }
    if (boost::regex_match(line.begin(), line.end(), match, memTotalRegex)) {
      folly::StringPiece numStr(
          line.begin() + match.position(1), size_t(match.length(1)));
      memTotal = folly::to<size_t>(numStr) * 1024;
    }
  };
  return (memAvailable && memTotal) ? memTotal - memAvailable : 0;

#else
  return 0;
#endif
}
} // namespace facebook::presto
