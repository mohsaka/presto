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

#include "presto_cpp/main/connectors/DeltaPrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"
#include "presto_cpp/presto_protocol/connector/delta/DeltaConnectorProtocol.h"

#include "presto_cpp/presto_protocol/connector/iceberg/IcebergConnectorProtocol.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/type/fbhive/HiveTypeParser.h"

namespace facebook::presto {

namespace {

template <typename T>
std::string toJsonString(const T& value) {
    return ((json)value).dump();
}

velox::connector::hive::HiveColumnHandle::ColumnType toHiveColumnType(protocol::delta::ColumnType deltaColumnType)
{
    switch (deltaColumnType) {
        case protocol::delta::ColumnType::REGULAR:
            return velox::connector::hive::HiveColumnHandle::ColumnType::kRegular;
        case protocol::delta::ColumnType::PARTITION:
            return velox::connector::hive::HiveColumnHandle::ColumnType::kPartitionKey;
        case protocol::delta::ColumnType::SUBFIELD:
            return velox::connector::hive::HiveColumnHandle::ColumnType::kSynthesized;
        default:
            VELOX_UNSUPPORTED(
                "Unsupported Hive column type: {}.", toJsonString(deltaColumnType));
    }
}

std::unique_ptr<velox::connector::ConnectorTableHandle> toDeltaTableHandle(
    const protocol::TupleDomain<protocol::Subfield>& domainPredicate,
    const std::shared_ptr<protocol::RowExpression>& remainingPredicate,
    bool isPushdownFilterEnabled,
    const std::string& tableName,
    const protocol::List<protocol::delta::DeltaColumn>& dataColumns,
    const protocol::TableHandle& tableHandle,
    const std::vector<velox::connector::hive::HiveColumnHandlePtr>&
        columnHandles,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) {
  // Create subfieldFilters here but don't pass by value
  velox::common::SubfieldFilters subfieldFilters;
  velox::core::TypedExprPtr remainingFilter = nullptr;

  velox::RowTypePtr finalDataColumns;
  if (!dataColumns.empty()) {
    std::vector<std::string> names;
    std::vector<velox::TypePtr> types;
    velox::type::fbhive::HiveTypeParser hiveTypeParser;
    names.reserve(dataColumns.size());
    types.reserve(dataColumns.size());
    for (auto& column : dataColumns) {
        if (!column.partition) {
            names.emplace_back(column.name);
            auto parsedType = hiveTypeParser.parse(column.type);
            types.push_back(VELOX_DYNAMIC_TYPE_DISPATCH(
                fieldNamesToLowerCase, parsedType->kind(), parsedType));
        }
    }
    finalDataColumns = ROW(std::move(names), std::move(types));
  }

  return std::make_unique<velox::connector::hive::HiveTableHandle>(
      tableHandle.connectorId,
      tableName,
      isPushdownFilterEnabled,
      std::move(subfieldFilters), // Move instead of copy
      std::move(remainingFilter), // Move instead of copy
      finalDataColumns,
      std::unordered_map<std::string, std::string>{},
      columnHandles);
}
} // namespace

// TODO: DONE. Review?
std::unique_ptr<velox::connector::ConnectorSplit>
DeltaPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
    auto deltaSplit =
        dynamic_cast<const protocol::delta::DeltaSplit*>(connectorSplit);
    VELOX_CHECK_NOT_NULL(
        deltaSplit, "Unexpected split type {}", connectorSplit->_type);

    // Convert Delta's partitionValues (Map<String, String>)
    // to Hive's partitionKeys (map<string, optional<string>>)
    std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
    for (const auto& [key, value] : deltaSplit->partitionValues) {
        // Delta partition values are always non-null strings,
        // but Hive uses optional to handle null partitions
        partitionKeys[key] = std::optional<std::string>(value);
    }

    std::unordered_map<std::string, std::string> customSplitInfo = {
        {"table_format", "delta"}
    };

    std::unordered_map<std::string, std::string> infoColumns =
        {{"$path", deltaSplit->filePath}};

    std::unordered_map<std::string, std::string> serdeParameters;

    return std::make_unique<velox::connector::hive::HiveConnectorSplit>(
      catalogId,
      deltaSplit->filePath,
      velox::dwio::common::FileFormat::PARQUET,
      deltaSplit->start,
      deltaSplit->length,
      partitionKeys,
      std::nullopt,
      customSplitInfo,
      nullptr,
      serdeParameters,
      0,
      splitContext->cacheable,
      infoColumns
      );

}

std::vector<velox::common::Subfield> toRequiredSubfields(
    const std::shared_ptr<protocol::Subfield>& subfield) {
    std::vector<velox::common::Subfield> result;
    if (subfield) {
        result.emplace_back(velox::common::Subfield(*subfield));
    }
    return result;
}

// TODO: This type parsing may not be correct. Need to check.
std::unique_ptr<velox::connector::ColumnHandle>
DeltaPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
    velox::type::fbhive::HiveTypeParser hiveTypeParser;
  auto deltaColumn =
      dynamic_cast<const protocol::delta::DeltaColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      deltaColumn, "Unexpected column handle type {}", column->_type);

  auto type = hiveTypeParser.parse(deltaColumn->dataType);
  velox::connector::hive::HiveColumnHandle::ColumnParseParameters
      columnParseParameters;
  if (type->isDate()) {
    columnParseParameters.partitionDateValueFormat = velox::connector::hive::
        HiveColumnHandle::ColumnParseParameters::kDaysSinceEpoch;
  }

  return std::make_unique<velox::connector::hive::HiveColumnHandle>(
      deltaColumn->name,
      toHiveColumnType(deltaColumn->columnType),
      type,
      type,
      toRequiredSubfields(deltaColumn->subfield),
      columnParseParameters);
}

// TODO: Not finished
std::unique_ptr<velox::connector::ConnectorTableHandle>
DeltaPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) const {
  auto deltaLayout = std::dynamic_pointer_cast<
      const protocol::delta::DeltaTableLayoutHandle>(
      tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      deltaLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);

  std::unordered_set<std::string> columnNames;
  std::vector<velox::connector::hive::HiveColumnHandlePtr> columnHandles;
  for (const auto& column : deltaLayout->table.deltaTable.columns) {
    if (columnNames.emplace(column.name).second) {
       // Delta column handle creation from DeltaMetadata.java:getColumnHandles()
       auto deltaColumnHandle = std::make_shared<protocol::delta::DeltaColumnHandle>();
        deltaColumnHandle->name = column.name;
        deltaColumnHandle->dataType = column.type;
        deltaColumnHandle->columnType =
            column.partition ? protocol::delta::ColumnType::PARTITION
                             : protocol::delta::ColumnType::REGULAR;
        deltaColumnHandle->subfield = nullptr;
      columnHandles.emplace_back(
          std::dynamic_pointer_cast<
              const velox::connector::hive::HiveColumnHandle>(
              std::shared_ptr(toVeloxColumnHandle(deltaColumnHandle.get(), typeParser))));
    }
  }

    // TODO: Figure out the predicate stuff
    /*
  // Add synthesized columns to the TableScanNode columnHandles as well.
  for (const auto& entry : icebergLayout->predicateColumns) {
    if (columnNames.emplace(entry.second.columnIdentity.name).second) {
      columnHandles.emplace_back(
          std::dynamic_pointer_cast<
              const velox::connector::hive::HiveColumnHandle>(
              std::shared_ptr(toVeloxColumnHandle(&entry.second, typeParser))));
    }
  }

  auto icebergTableHandle =
      std::dynamic_pointer_cast<const protocol::iceberg::IcebergTableHandle>(
          tableHandle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      icebergTableHandle,
      "Unexpected table handle type {}",
      tableHandle.connectorHandle->_type);*/

  // Use fully qualified name if available.
    auto deltaTable = deltaLayout->table.deltaTable;
  std::string tableName = deltaTable.schemaName.empty()
      ?deltaTable.tableName
      : fmt::format(
            "{}.{}",
            deltaTable.schemaName,
            deltaTable.tableName);

    protocol::TupleDomain<protocol::Subfield> temp;
    std::shared_ptr<protocol::RowExpression> tempPredicates;
  return toDeltaTableHandle(
      temp, // TODO: predicates
      tempPredicates, // TODO: Predicates
      false, // TODO: We need to get config. See Java TestDeltaScanOptimizations.
      tableName,
      deltaLayout->table.deltaTable.columns,
      tableHandle,
      columnHandles,
      exprConverter,
      typeParser);
}

std::unique_ptr<protocol::ConnectorProtocol>
DeltaPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::delta::DeltaConnectorProtocol>();
}

// TODO: Insert is not supported
/*
std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
IcebergPrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::CreateHandle* createHandle,
    const TypeParser& typeParser) const {
  auto icebergOutputTableHandle =
      std::dynamic_pointer_cast<protocol::iceberg::IcebergOutputTableHandle>(
          createHandle->handle.connectorHandle);

  VELOX_CHECK_NOT_NULL(
      icebergOutputTableHandle,
      "Unexpected output table handle type {}",
      createHandle->handle.connectorHandle->_type);

  const auto inputColumns =
      toHiveColumns(icebergOutputTableHandle->inputColumns, typeParser);

  return std::make_unique<
      velox::connector::hive::iceberg::IcebergInsertTableHandle>(
      inputColumns,
      std::make_shared<velox::connector::hive::LocationHandle>(
          fmt::format("{}/data", icebergOutputTableHandle->outputPath),
          fmt::format("{}/data", icebergOutputTableHandle->outputPath),
          velox::connector::hive::LocationHandle::TableType::kNew),
      toVeloxFileFormat(icebergOutputTableHandle->fileFormat),
      toVeloxIcebergPartitionSpec(
          icebergOutputTableHandle->partitionSpec, typeParser),
      std::optional(
          toFileCompressionKind(icebergOutputTableHandle->compressionCodec)));
}

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
IcebergPrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::InsertHandle* insertHandle,
    const TypeParser& typeParser) const {
  auto icebergInsertTableHandle =
      std::dynamic_pointer_cast<protocol::iceberg::IcebergInsertTableHandle>(
          insertHandle->handle.connectorHandle);

  VELOX_CHECK_NOT_NULL(
      icebergInsertTableHandle,
      "Unexpected insert table handle type {}",
      insertHandle->handle.connectorHandle->_type);

  const auto inputColumns =
      toHiveColumns(icebergInsertTableHandle->inputColumns, typeParser);

  return std::make_unique<
      velox::connector::hive::iceberg::IcebergInsertTableHandle>(
      inputColumns,
      std::make_shared<velox::connector::hive::LocationHandle>(
          fmt::format("{}/data", icebergInsertTableHandle->outputPath),
          fmt::format("{}/data", icebergInsertTableHandle->outputPath),
          velox::connector::hive::LocationHandle::TableType::kExisting),
      toVeloxFileFormat(icebergInsertTableHandle->fileFormat),
      toVeloxIcebergPartitionSpec(
          icebergInsertTableHandle->partitionSpec, typeParser),
      std::optional(
          toFileCompressionKind(icebergInsertTableHandle->compressionCodec)));
}

std::vector<velox::connector::hive::HiveColumnHandlePtr>
DeltaPrestoToVeloxConnector::toHiveColumns(
    const protocol::List<protocol::iceberg::DeltaColumnHandle>& inputColumns,
    const TypeParser& typeParser) const {
  std::vector<velox::connector::hive::HiveColumnHandlePtr> hiveColumns;
  hiveColumns.reserve(inputColumns.size());
  for (const auto& columnHandle : inputColumns) {
    hiveColumns.emplace_back(
        std::dynamic_pointer_cast<velox::connector::hive::HiveColumnHandle>(
            std::shared_ptr(toVeloxColumnHandle(&columnHandle, typeParser))));
  }
  return hiveColumns;
}*/

} // namespace facebook::presto
