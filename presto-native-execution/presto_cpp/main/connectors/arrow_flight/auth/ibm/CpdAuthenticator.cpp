#include "presto_cpp/main/connectors/arrow_flight/auth/ibm/CpdAuthenticator.h"
#include <arrow/flight/api.h>

namespace facebook::presto::ibm {

void CpdAuthenticator::authenticateClient(
    std::unique_ptr<arrow::flight::FlightClient>& client,
    const velox::config::ConfigBase* sessionProperties,
    const std::map<std::string, std::string>& extraCredentials,
    arrow::flight::AddCallHeaders& headerWriter) {
  VELOX_USER_CHECK(
      extraCredentials.find(CpdAuthenticator::kCpdTokenKey) !=
          extraCredentials.end(),
      "Arrow flight server token is missing from extra credentials");
  const std::string& token =
      extraCredentials.at(CpdAuthenticator::kCpdTokenKey);
  headerWriter.AddHeader(
      CpdAuthenticator::kCpdAuthorizationKey, "Bearer " + token);
}

AFC_REGISTER_AUTH_FACTORY(std::make_shared<CpdAuthenticatorFactory>())

} // namespace facebook::presto::ibm
