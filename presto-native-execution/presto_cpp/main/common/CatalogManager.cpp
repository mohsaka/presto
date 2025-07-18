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

#include "presto_cpp/main/common/CatalogManager.h"
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"
#include "proxygen/httpserver/ResponseBuilder.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto {

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

// Replaces strings of the form "${VAR}"
// with the value of the environment variable "VAR" (if it exists).
// Does nothing if the input doesn't look like "${...}".
void extractValueIfEnvironmentVariable(std::string& value) {
  if (value.size() > 3 && value.substr(0, 2) == "${" && value.back() == '}') {
    auto envName = value.substr(2, value.size() - 3);

    const char* envVal = std::getenv(envName.c_str());
    if (envVal != nullptr) {
      if (strlen(envVal) == 0) {
        LOG(WARNING) << fmt::format(
            "Config environment variable {} is empty.", envName);
      }
      value = std::string(envVal);
    }
  }
}

void CatalogManager::registerCatalog(
    const std::string& catalogName,
    std::unordered_map<std::string, std::string> connectorConf) {
  std::shared_ptr<const velox::config::ConfigBase> properties =
      std::make_shared<const velox::config::ConfigBase>(
          std::move(connectorConf));

  auto connectorName = util::requiredProperty(*properties, kConnectorName);

  catalogNames_->emplace_back(catalogName);

  LOG(INFO) << "Registering catalog " << catalogName << " using connector "
            << connectorName;

  // Make sure that the connector type is supported.
  getPrestoToVeloxConnector(connectorName);

  std::shared_ptr<velox::connector::Connector> connector =
      velox::connector::getConnectorFactory(connectorName)
          ->newConnector(
              catalogName,
              std::move(properties),
              connectorIoExecutor_,
              connectorCpuExecutor_);
  VELOX_CHECK_NOT_NULL(connector, "Connector is null.");
  velox::connector::registerConnector(connector);
}

void CatalogManager::registerCatalogFromJson(
    const proxygen::HTTPMessage* message,
    const std::vector<std::unique_ptr<folly::IOBuf>>& body,
    proxygen::ResponseHandler* downstream,
    Announcer* announcer) {
  std::string catalogName;
  std::ostringstream propertiesString;

  try {
    const auto path = message->getPath();
    const auto lastSlash = path.find_last_of('/');
    catalogName = path.substr(lastSlash + 1);

    VELOX_USER_CHECK(
        find(catalogNames_->begin(), catalogNames_->end(), catalogName) ==
            catalogNames_->end(),
        fmt::format("Catalog ['{}'] is already present.", catalogName));

    auto json = nlohmann::json::parse(util::extractMessageBody(body));
    VELOX_USER_CHECK(json.is_object(), "Not a JSON object.");

    auto connectorConf = readConfigFromJson(json, propertiesString);

    LOG(INFO)
        << "Registered catalog property keys from in-memory JSON for catalog '"
        << catalogName << "':\n"
        << logConnectorConfigPropertyKeys(connectorConf);

    registerCatalog(catalogName, std::move(connectorConf));

    // Update and force an announcement to let the coordinator know about the
    // new catalog.
    announcer->updateConnectorIds(catalogNames_);
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
      writeConfigToFile(propertyFile, propertiesString.str());
    } catch (const std::exception& ex) {
      LOG(WARNING) << fmt::format(
          "Failed to write catalog file %s: %s",
          propertyFile.string(),
          ex.what());
    }
  }
}

void CatalogManager::registerCatalogsFromPath(
    const fs::path& configDirectoryPath) {
  for (const auto& entry : fs::directory_iterator(configDirectoryPath)) {
    if (entry.path().extension() == kPropertiesExtension) {
      auto fileName = entry.path().filename().string();
      auto catalogName =
          fileName.substr(0, fileName.size() - kPropertiesExtension.size());

      auto connectorConf = util::readConfig(entry.path());
      PRESTO_STARTUP_LOG(INFO)
          << "Registered catalog property keys from " << entry.path() << ":\n"
          << logConnectorConfigPropertyKeys(connectorConf);
      registerCatalog(catalogName, std::move(connectorConf));
    }
  }
}

std::unordered_map<std::string, std::string> CatalogManager::readConfigFromJson(
    const nlohmann::json& json,
    std::ostringstream& propertiesString) {
  std::unordered_map<std::string, std::string> config;
  for (auto it = json.begin(); it != json.end(); ++it) {
    VELOX_USER_CHECK(
        it.value().is_string(),
        fmt::format(
            "Value for key '{}' must be a string, but got: {}",
            it.key(),
            it.value().dump()));
    propertiesString << it.key() << "=" << it.value().get<std::string>()
                     << "\n";

    // Fill in the mapping for in-memory catalog creation.
    auto value = it.value().get<std::string>();
    extractValueIfEnvironmentVariable(value);
    config.emplace(it.key(), value);
  }
  return config;
}

void CatalogManager::writeConfigToFile(
    const fs::path& propertyFile,
    const std::string& config) {
  // This function runs when the config is not successfully written.
  // If a partial file or some corruption occurs, it is deleted.
  auto removePropertyFile = [](const std::filesystem::path& propertyFile) {
    std::error_code ec;
    if (std::filesystem::remove(propertyFile, ec)) {
      LOG(INFO) << "Removed file " << propertyFile;
    } else {
      LOG(WARNING) << "Failed to remove file " << propertyFile
                   << ". Error: " << ec.message();
    }
  };
  std::ofstream out(propertyFile);
  if (!out.is_open()) {
    LOG(WARNING) << "Unable to open file " << propertyFile
                 << ". Catalog will be in memory only.";
    return;
  }

  out << config;
  if (out.fail()) {
    LOG(WARNING) << "Unable to write to file " << propertyFile
                 << ". Catalog will be in memory only.";
    removePropertyFile(propertyFile);
    return;
  }

  out.close();

  // If something goes wrong while closing, for example, buffer flush fails or
  // disk is full, attempt to clean up the file.
  if (out.fail() || out.bad()) {
    LOG(WARNING) << "Unable to close file " << propertyFile
                 << ". Catalog will be in memory only.";
    removePropertyFile(propertyFile);
  }
}

} // namespace facebook::presto
