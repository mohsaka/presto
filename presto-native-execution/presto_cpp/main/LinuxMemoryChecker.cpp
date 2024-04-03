#include "presto_cpp/main/LinuxMemoryChecker.h"
#ifdef __linux__
#include <sys/sysinfo.h>
#include <sys/types.h>
#endif

namespace facebook::presto {
int64_t LinuxMemoryChecker::systemUsedMemoryBytes() const {
#ifdef __linux__
  struct sysinfo memInfo;
  int64_t inUseMemory = 0;
  sysinfo(&memInfo);
  return (memInfo.totalram - memInfo.freeram) * memInfo.mem_unit;
#else
  return 0;
#endif
}
} // namespace facebook::presto
