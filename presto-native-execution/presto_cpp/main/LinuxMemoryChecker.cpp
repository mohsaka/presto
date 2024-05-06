#include "presto_cpp/main/LinuxMemoryChecker.h"
#ifdef __linux__
#include <boost/regex.hpp>
#include <folly/gen/Base.h>
#include <folly/gen/File.h>
#include <folly/gen/String.h>

#include <folly/Conv.h>
#include <folly/CppAttributes.h>
#include <folly/Format.h>
#include <folly/Range.h>
#include <folly/String.h>
#endif

namespace facebook::presto {
int64_t LinuxMemoryChecker::systemUsedMemoryBytes() {
#ifdef __linux__
  static const boost::regex memAvailableRegex(R"!(MemAvailable:\s*(\d+)\s*kB)!");
  static const boost::regex memTotalRegex(R"!(MemTotal:\s*(\d+)\s*kB)!");
  size_t memAvailable = 0;
  size_t memTotal = 0;
  boost::cmatch match;

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
