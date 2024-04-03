#include "presto_cpp/main/PeriodicMemoryChecker.h"

namespace facebook::presto {
class LinuxMemoryChecker : public PeriodicMemoryChecker {
 public:
  explicit LinuxMemoryChecker(
      PeriodicMemoryChecker::Config config,
      int64_t mallocBytes = 0,
      std::function<void()>&& periodicCb = nullptr,
      std::function<bool(const std::string&)>&& heapDumpCb = nullptr)
      : PeriodicMemoryChecker(config),
        mallocBytes_(mallocBytes),
        periodicCb_(std::move(periodicCb)),
        heapDumpCb_(std::move(heapDumpCb)) {}

  void setMallocBytes(int64_t mallocBytes) {
    mallocBytes_ = mallocBytes;
  }

 protected:
  int64_t systemUsedMemoryBytes() const override;

  int64_t mallocBytes() const override {
    return mallocBytes_;
  }

  void periodicCb() const override {
    if (periodicCb_) {
      periodicCb_();
    }
  }

  bool heapDumpCb(const std::string& filePath) const override {
    if (heapDumpCb_) {
      return heapDumpCb_(filePath);
    }
    return false;
  }

  void removeDumpFile(const std::string& filePath) const override {}

 private:
  int64_t mallocBytes_{0};
  std::function<void()> periodicCb_;
  std::function<bool(const std::string&)> heapDumpCb_;
};
} // namespace facebook::presto
