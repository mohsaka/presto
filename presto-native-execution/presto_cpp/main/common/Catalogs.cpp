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

#include "presto_cpp/main/common/Catalogs.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "presto_cpp/main/Announcer.h"
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"
#include "proxygen/httpserver/ResponseBuilder.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::catalog {

constexpr std::string_view kPropertiesExtension = ".properties";
constexpr char const* kConnectorName = "connector.name";

// Log only the catalog keys that are configured to avoid leaking
// secret information. Some values represent secrets used to access
// storage backends.
std::string logConnectorConfigPropertyKeys(
    const std::unordered_map<std::string, std::string>& configs) {
  std::stringstream out;
  for (auto const& [key, value] : configs) {
    out << "  " << key << "\n";
  }
  return out.str();
}

void registerCatalog(
    const std::string& catalogName,
    std::unordered_map<std::string, std::string> connectorConf,
    CatalogContext& ctx) {
  std::shared_ptr<const velox::config::ConfigBase> properties =
      std::make_shared<const velox::config::ConfigBase>(
          std::move(connectorConf));

  auto connectorName = util::requiredProperty(*properties, kConnectorName);

  ctx.catalogNames.emplace_back(catalogName);

  LOG(INFO) << "Registering catalog " << catalogName << " using connector "
            << connectorName;

  // Make sure that the connector type is supported.
  getPrestoToVeloxConnector(connectorName);

  std::shared_ptr<velox::connector::Connector> connector =
      velox::connector::getConnectorFactory(connectorName)
          ->newConnector(
              catalogName,
              std::move(properties),
              ctx.connectorIoExecutor,
              ctx.connectorCpuExecutor);
  VELOX_CHECK_NOT_NULL(connector, "Connector is null.");
  velox::connector::registerConnector(connector);
}

void registerCatalogFromJson(
    const proxygen::HTTPMessage* message,
    const std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream,
    CatalogContext& ctx,
    Announcer* announcer) {
  std::string catalogName;
  std::ostringstream propertiesString;

  try {
    const auto path = message->getPath();
    const auto lastSlash = path.find_last_of('/');
    catalogName = path.substr(lastSlash + 1);

    VELOX_USER_CHECK(
        find(ctx.catalogNames.begin(), ctx.catalogNames.end(), catalogName) ==
            ctx.catalogNames.end(),
        fmt::format("Catalog ['{}'] is already present.", catalogName));

    auto json = nlohmann::json::parse(util::extractMessageBody(body));
    VELOX_USER_CHECK(json.is_object(), "Not a JSON object.");

    auto connectorConf = util::readConfigFromJson(json, propertiesString);

    LOG(INFO)
        << "Registered catalog property keys from in-memory JSON for catalog '"
        << catalogName << "':\n"
        << logConnectorConfigPropertyKeys(connectorConf);

    registerCatalog(catalogName, std::move(connectorConf), ctx);

    // Update and force an announcement to let the coordinator know about the
    // new catalog.
    announcer->updateConnectorIds(ctx.catalogNames);
    announcer->sendRequest();

    proxygen::ResponseBuilder(downstream)
        .status(http::kHttpOk, "OK")
        .body("Registered catalog: " + catalogName)
        .sendWithEOM();
  } catch (const std::exception& ex) {
    proxygen::ResponseBuilder(downstream)
        .status(http::kHttpBadRequest, "Bad Request")
        .body(std::string("Catalog registration failed: ") + ex.what())
        .sendWithEOM();
    return;
  }

  if (!SystemConfig::instance()->dynamicCatalogPath().empty()) {
    const fs::path propertyFile =
        SystemConfig::instance()->dynamicCatalogPath() + catalogName +
        std::string(kPropertiesExtension);
    try {
      util::writeConfigToFile(propertyFile, propertiesString.str());
    } catch (const std::exception& ex) {
      LOG(WARNING) << fmt::format(
          "Failed to write catalog file %s: %s",
          propertyFile.string(),
          ex.what());
    }
  }
}

void registerCatalogsFromPath(
    const fs::path& configDirectoryPath,
    CatalogContext& ctx) {
  for (const auto& entry : fs::directory_iterator(configDirectoryPath)) {
    if (entry.path().extension() == kPropertiesExtension) {
      auto fileName = entry.path().filename().string();
      auto catalogName =
          fileName.substr(0, fileName.size() - kPropertiesExtension.size());

      auto connectorConf = util::readConfig(entry.path());
      PRESTO_STARTUP_LOG(INFO)
          << "Registered catalog property keys from " << entry.path() << ":\n"
          << logConnectorConfigPropertyKeys(connectorConf);
      registerCatalog(catalogName, std::move(connectorConf), ctx);
    }
  }
}
} // namespace facebook::presto::catalog
