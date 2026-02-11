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

#include "TestingTableFunctions.h"

#include "velox/vector/BaseVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace facebook::presto::tvf {

// Operator-based SimpleTableFunction implementation
std::unique_ptr<SimpleTableFunctionAnalysis> SimpleTableFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  std::vector<std::string> returnNames;
  std::vector<TypePtr> returnTypes;

  const auto arg = std::dynamic_pointer_cast<ScalarArgument>(args.at("COLUMN"));
  const auto val = arg->value()->as<ConstantVector<StringView>>()->valueAt(0);

  returnNames.push_back(val);
  returnTypes.push_back(BOOLEAN());

  auto analysis = std::make_unique<SimpleTableFunctionAnalysis>();
  analysis->tableFunctionHandle_ =
      std::make_shared<SimpleTableFunctionHandle>(std::string(val));
  analysis->returnType_ =
      std::make_shared<Descriptor>(returnNames, returnTypes);
  return analysis;
}

void registerSimpleTableFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<ScalarArgumentSpecification>("COLUMN", VARCHAR(), false, "col"));
  argSpecs.insert(
      std::make_shared<ScalarArgumentSpecification>(
          "IGNORED", BIGINT(), false, "0"));

  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<GenericTableReturnTypeSpecification>(),
      SimpleTableFunction::analyze,
      TableFunction::defaultCreateDataProcessor,
      TableFunction::defaultCreateSplitProcessor,
      SimpleTableFunction::getSplits);
}

std::shared_ptr<TableFunctionResult> IdentityDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  auto numRows = inputTable->size();
  if (numRows == 0) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  return std::make_shared<TableFunctionResult>(true, std::move(inputTable));
}

std::unique_ptr<IdentityFunctionAnalysis> IdentityFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));
  std::vector<std::string> returnNames = input->rowType()->names();
  std::vector<TypePtr> returnTypes;
  std::vector<column_index_t> requiredColsList;
  for (size_t i = 0; i < returnNames.size(); i++) {
    returnTypes.push_back(input->rowType()->childAt(i));
    requiredColsList.push_back(i);
  }
  RequiredColumnsMap requiredColumns;
  requiredColumns.emplace("INPUT", requiredColsList);

  auto analysis = std::make_unique<IdentityFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<IdentityFunctionHandle>();
  analysis->returnType_ =
      std::make_shared<Descriptor>(returnNames, returnTypes);
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerIdentityFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, false));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<GenericTableReturnTypeSpecification>(),
      IdentityFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<IdentityDataProcessor>(
            dynamic_cast<const IdentityFunctionHandle*>(handle.get()), pool);
      });
}

std::shared_ptr<TableFunctionResult> RepeatFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  auto numRows = inputTable->size();

  if (numRows == 0) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }

  auto count = handle_->count();
  auto totalOutputRows = numRows * count;

  // Create index column - repeat each row index 'count' times
  auto indexColumn = BaseVector::create<FlatVector<int32_t>>(INTEGER(), totalOutputRows, pool());
  auto* rawIndices = indexColumn->mutableRawValues();

  for (int64_t round = 0; round < count; round++) {
    for (int64_t i = 0; i < numRows; i++) {
      rawIndices[round * numRows + i] = i;
    }
  }

  // Create output RowVector
  auto rowType = ROW({INTEGER()});
  std::vector<VectorPtr> children = {indexColumn};
  RowVectorPtr outputTable = std::make_shared<RowVector>(
      pool(), rowType, BufferPtr(nullptr), totalOutputRows, std::move(children));

  return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
}

std::unique_ptr<RepeatFunctionAnalysis> RepeatFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));

  auto countArg = args.at("COUNT");
  auto countPtr = std::dynamic_pointer_cast<ScalarArgument>(countArg);

  auto count = countPtr->value()->as<ConstantVector<int64_t>>()->valueAt(0);

  // For ONLY_PASS_THROUGH, per spec, function must require at least one column (index 0)
  RequiredColumnsMap requiredColumns{{"INPUT", {0}}};

  auto analysis = std::make_unique<RepeatFunctionAnalysis>();
  analysis->tableFunctionHandle_ =
      std::make_shared<RepeatFunctionHandle>(count);
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerRepeatFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, true));
  argSpecs.insert(
      std::make_shared<ScalarArgumentSpecification>("COUNT", BIGINT(), false, "2"));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<OnlyPassThroughReturnTypeSpecification>(),
      RepeatFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<RepeatFunctionDataProcessor>(
            dynamic_cast<const RepeatFunctionHandle*>(handle.get()), pool);
      });
}

std::shared_ptr<TableFunctionResult> IdentityPassThroughFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  
  // Handle null input table (happens when partition has 0 rows)
  auto numRows = inputTable ? inputTable->size() : 0;

  // Create index column with identity mapping (0, 1, 2, ...)
  auto indexColumn = BaseVector::create<FlatVector<int32_t>>(INTEGER(), numRows, pool());

  if (numRows > 0) {
    auto* rawIndices = indexColumn->mutableRawValues();
    std::iota(rawIndices, rawIndices + numRows, 0);
  }

  // Create output RowVector
  auto rowType = ROW({INTEGER()});
  std::vector<VectorPtr> children = {indexColumn};
  RowVectorPtr outputTable = std::make_shared<RowVector>(
      pool(), rowType, BufferPtr(nullptr), numRows, std::move(children));

  return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
}

std::unique_ptr<IdentityPassThroughFunctionAnalysis> IdentityPassThroughFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));

  // Per spec, function must require at least one column (index 0)
  RequiredColumnsMap requiredColumns{{"INPUT", {0}}};

  auto analysis = std::make_unique<IdentityPassThroughFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<IdentityPassThroughFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerIdentityPassThroughFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, true));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<OnlyPassThroughReturnTypeSpecification>(),
      IdentityPassThroughFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<IdentityPassThroughFunctionDataProcessor>(
            dynamic_cast<const IdentityPassThroughFunctionHandle*>(handle.get()), pool);
      });
}

std::shared_ptr<TableFunctionResult> EmptyOutputFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  
  // Return empty output (0 rows, 1 BOOLEAN column) for each input page
  // This matches the Java implementation which returns an empty Page
  auto rowType = ROW({BOOLEAN()});
  std::vector<VectorPtr> children = {
      BaseVector::create<FlatVector<bool>>(BOOLEAN(), 0, pool())};
  RowVectorPtr outputTable = std::make_shared<RowVector>(
      pool(), rowType, BufferPtr(nullptr), 0, std::move(children));

  return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
}

std::unique_ptr<EmptyOutputFunctionAnalysis> EmptyOutputFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));

  // Require all input columns (per Java implementation)
  std::vector<column_index_t> requiredColsList;
  for (size_t i = 0; i < input->rowType()->size(); i++) {
    requiredColsList.push_back(i);
  }
  RequiredColumnsMap requiredColumns;
  requiredColumns.emplace("INPUT", requiredColsList);

  auto analysis = std::make_unique<EmptyOutputFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<EmptyOutputFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerEmptyOutputFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, false));  // rowSemantics=false, pruneWhenEmpty=false (keepWhenEmpty), passThroughColumns=false
  
  // Create descriptor for the return type: single BOOLEAN column named "column"
  std::vector<std::string> returnNames = {"column"};
  std::vector<TypePtr> returnTypes = {BOOLEAN()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      EmptyOutputFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<EmptyOutputFunctionDataProcessor>(
            dynamic_cast<const EmptyOutputFunctionHandle*>(handle.get()), pool);
      });
}

} // namespace facebook::presto::tvf

