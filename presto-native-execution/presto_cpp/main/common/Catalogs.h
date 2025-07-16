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

#include <proxygen/httpserver/RequestHandlerFactory.h>
#include "presto_cpp/main/PrestoServer.h"
#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

namespace facebook::presto::catalog {

struct CatalogContext {
  std::vector<std::string>& catalogNames;
  folly::IOThreadPoolExecutor* connectorIoExecutor;
  folly::CPUThreadPoolExecutor* connectorCpuExecutor;

  CatalogContext(
      std::vector<std::string>& names,
      folly::IOThreadPoolExecutor* io,
      folly::CPUThreadPoolExecutor* cpu)
      : catalogNames(names),
        connectorIoExecutor(io),
        connectorCpuExecutor(cpu) {}
};

void registerCatalog(
    const std::string& catalogName,
    std::unordered_map<std::string, std::string> connectorConf,
    CatalogContext& ctx);

void registerCatalogFromJson(
    const proxygen::HTTPMessage* message,
    const std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream,
    CatalogContext& ctx,
    Announcer* announcer);

void registerCatalogsFromPath(
    const fs::path& configDirectoryPath,
    CatalogContext& ctx);
} // namespace facebook::presto::catalog
