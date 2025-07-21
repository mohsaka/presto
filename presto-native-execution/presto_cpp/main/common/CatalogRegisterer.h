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
#pragma once

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/Announcer.h"

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

namespace facebook::presto {

struct CatalogRegisterer {
  CatalogRegisterer()
      : connectorIoExecutor_(nullptr),
        connectorCpuExecutor_(nullptr),
        catalogNames_(nullptr) {}

  void init(
      folly::IOThreadPoolExecutor* connectorIoExecutor,
      folly::CPUThreadPoolExecutor* connectorCpuExecutor,
      std::vector<std::string>* catalogNames);

  void registerCatalogFromJson(
      const proxygen::HTTPMessage* message,
      const std::vector<std::unique_ptr<folly::IOBuf>>& body,
      proxygen::ResponseHandler* downstream,
      Announcer* announcer);

  void registerCatalogsFromPath(const fs::path& configDirectoryPath);

 private:
  // PrestoServer owns these objects, and these only point to them.
  folly::IOThreadPoolExecutor* connectorIoExecutor_;
  folly::CPUThreadPoolExecutor* connectorCpuExecutor_;
  std::vector<std::string>* catalogNames_;

  void registerCatalog(
      const std::string& catalogName,
      std::unordered_map<std::string, std::string> connectorConf);

  std::unordered_map<std::string, std::string> readConfigFromJson(
      const nlohmann::json& json,
      std::ostringstream& propertiesString);

  void writeConfigToFile(
      const fs::path& propertyFile,
      const std::string& config);
};

} // namespace facebook::presto
