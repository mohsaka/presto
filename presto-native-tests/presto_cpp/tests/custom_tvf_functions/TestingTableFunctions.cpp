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

std::shared_ptr<TableFunctionResult> EmptyOutputWithPassThroughFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  
  // Handle null input (empty partition)
  if (!inputTable) {
    return std::make_shared<TableFunctionResult>(true, nullptr);
  }
  
  // Return empty output with 2 columns: 1 proper BOOLEAN + 1 pass-through BIGINT index
  // This matches Java which returns a Page with 2 blocks, even though the return type
  // specification only declares 1 proper column. The second column is the pass-through index.
  auto rowType = ROW({BOOLEAN(), BIGINT()});
  std::vector<VectorPtr> children = {
      BaseVector::create<FlatVector<bool>>(BOOLEAN(), 0, pool()),
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 0, pool())};
  RowVectorPtr outputTable = std::make_shared<RowVector>(
      pool(), rowType, BufferPtr(nullptr), 0, std::move(children));

  return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
}

std::unique_ptr<EmptyOutputWithPassThroughFunctionAnalysis> EmptyOutputWithPassThroughFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));

  // Require all input columns (per Java implementation)
  std::vector<column_index_t> requiredColsList;
  for (size_t i = 0; i < input->rowType()->size(); i++) {
    requiredColsList.push_back(i);
  }
  RequiredColumnsMap requiredColumns;
  requiredColumns.emplace("INPUT", requiredColsList);

  // Return type is specified in DescribedTableReturnTypeSpecification at registration
  // Do not set returnType_ here to avoid ambiguity
  // The framework will automatically add pass-through columns

  auto analysis = std::make_unique<EmptyOutputWithPassThroughFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<EmptyOutputWithPassThroughFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerEmptyOutputWithPassThroughFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, true));  // rowSemantics=false, pruneWhenEmpty=false (keepWhenEmpty), passThroughColumns=true
  
  // Create descriptor for the return type: single BOOLEAN column named "column"
  // The framework will automatically add pass-through columns
  std::vector<std::string> returnNames = {"column"};
  std::vector<TypePtr> returnTypes = {BOOLEAN()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      EmptyOutputWithPassThroughFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<EmptyOutputWithPassThroughFunctionDataProcessor>(
            dynamic_cast<const EmptyOutputWithPassThroughFunctionHandle*>(handle.get()), pool);
      });
}
// EmptySourceFunction implementation
std::shared_ptr<TableFunctionResult> EmptySourceFunctionSplitProcessor::apply(
    const TableSplitHandlePtr& split) {
  
  // Return kFinished state to indicate we're done (no rows to produce)
  // This matches the Java implementation which returns empty results
  return std::make_shared<TableFunctionResult>(
      TableFunctionResult::TableFunctionState::kFinished);
}

std::vector<TableSplitHandlePtr> EmptySourceFunction::getSplits(
    const TableFunctionHandlePtr& handle) {
  // Return empty vector - no splits means no rows
  return std::vector<TableSplitHandlePtr>();
}

std::unique_ptr<EmptySourceFunctionAnalysis> EmptySourceFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  // No input arguments for source function
  
  // Return type is specified in DescribedTableReturnTypeSpecification at registration
  // Do not set returnType_ here to avoid ambiguity

  auto analysis = std::make_unique<EmptySourceFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<EmptySourceFunctionHandle>();
  // No required columns since there are no input arguments
  return analysis;
}

void registerEmptySourceFunction(const std::string& name) {
  // Source functions have no table arguments
  TableArgumentSpecList argSpecs;
  
  // Create descriptor for the return type: single BOOLEAN column named "column"
  std::vector<std::string> returnNames = {"column"};
  std::vector<TypePtr> returnTypes = {BOOLEAN()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      EmptySourceFunction::analyze,
      TableFunction::defaultCreateDataProcessor,  // No data processor
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionSplitProcessor> {
        return std::make_unique<EmptySourceFunctionSplitProcessor>(
            dynamic_cast<const EmptySourceFunctionHandle*>(handle.get()), pool);
      },
      EmptySourceFunction::getSplits);  // Add getSplits function
}

// ConstantFunction implementation
std::shared_ptr<TableFunctionResult> ConstantFunctionSplitProcessor::apply(
    const TableSplitHandlePtr& split) {
  bool usedData = false;

  // NOTE: The C++ framework passes the split on every apply() call until kFinished is returned.
  // This differs from Java where the split is passed once and subsequent calls receive null.
  // We use initialized_ flag to ensure we only process the split data once, preventing
  // state variables (fullPagesCount_, processedPages_, reminder_) from resetting on each call.
  if (split != nullptr && !initialized_) {
    auto constantSplit = dynamic_cast<const ConstantFunctionSplitHandle*>(split.get());
    int32_t count = constantSplit->count();
    fullPagesCount_ = count / PAGE_SIZE;
    reminder_ = count % PAGE_SIZE;
    initialized_ = true;
    if (fullPagesCount_ > 0) {
      auto flatVector = BaseVector::create<FlatVector<int32_t>>(INTEGER(), PAGE_SIZE, pool());
      
      if (!handle_->value().has_value()) {
        // Set all values to null
        for (int32_t i = 0; i < PAGE_SIZE; i++) {
          flatVector->setNull(i, true);
        }
      } else {
        // Set all values to the constant
        auto* rawValues = flatVector->mutableRawValues();
        int32_t value = static_cast<int32_t>(handle_->value().value());
        for (int32_t i = 0; i < PAGE_SIZE; i++) {
          rawValues[i] = value;
        }
      }
      block_ = flatVector;
    } else {
      auto flatVector = BaseVector::create<FlatVector<int32_t>>(INTEGER(), reminder_, pool());
      
      if (!handle_->value().has_value()) {
        // Set all values to null
        for (int32_t i = 0; i < reminder_; i++) {
          flatVector->setNull(i, true);
        }
      } else {
        // Set all values to the constant
        auto* rawValues = flatVector->mutableRawValues();
        int32_t value = static_cast<int32_t>(handle_->value().value());
        for (int32_t i = 0; i < reminder_; i++) {
          rawValues[i] = value;
        }
      }
      block_ = flatVector;
    }
    usedData = true;
  }

  if (processedPages_ < fullPagesCount_) {
    processedPages_++;
    auto rowType = ROW({INTEGER()});
    std::vector<VectorPtr> children = {block_};
    RowVectorPtr outputTable = std::make_shared<RowVector>(
        pool(), rowType, BufferPtr(nullptr), PAGE_SIZE, std::move(children));
    
    if (usedData) {
      return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
    }
    return std::make_shared<TableFunctionResult>(false, std::move(outputTable));
  }

  if (reminder_ > 0) {
    // If fullPagesCount_ > 0, we need to slice the PAGE_SIZE block to get the reminder
    // If fullPagesCount_ == 0, the block is already reminder_ size, so use it directly
    VectorPtr outputVector;
    if (fullPagesCount_ > 0) {
      outputVector = block_->slice(0, reminder_);
    } else {
      outputVector = block_;
    }
    
    auto rowType = ROW({INTEGER()});
    std::vector<VectorPtr> children = {outputVector};
    RowVectorPtr outputTable = std::make_shared<RowVector>(
        pool(), rowType, BufferPtr(nullptr), reminder_, std::move(children));
    reminder_ = 0;
    
    if (usedData) {
      return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
    }
    return std::make_shared<TableFunctionResult>(false, std::move(outputTable));
  }

  return std::make_shared<TableFunctionResult>(
      TableFunctionResult::TableFunctionState::kFinished);
}

std::vector<TableSplitHandlePtr> ConstantFunction::getSplits(
    const TableFunctionHandlePtr& handle) {
  auto constantHandle = dynamic_cast<const ConstantFunctionHandle*>(handle.get());
  constexpr int32_t DEFAULT_SPLIT_SIZE = 5500;
  
  std::vector<TableSplitHandlePtr> splits;
  int32_t count = constantHandle->count();
  
  for (int32_t i = 0; i < count / DEFAULT_SPLIT_SIZE; i++) {
    splits.push_back(std::make_shared<ConstantFunctionSplitHandle>(DEFAULT_SPLIT_SIZE));
  }
  
  int32_t remainingSize = count % DEFAULT_SPLIT_SIZE;
  if (remainingSize > 0) {
    splits.push_back(std::make_shared<ConstantFunctionSplitHandle>(remainingSize));
  }
  
  return splits;
}

std::unique_ptr<ConstantFunctionAnalysis> ConstantFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  // Get VALUE argument (can be null)
  auto valueArg = std::dynamic_pointer_cast<ScalarArgument>(args.at("VALUE"));
  std::optional<int32_t> value;
  
  // Check if the value vector exists and is not null at position 0
  // INTEGER type in Presto maps to int32_t in Velox
  auto valueVector = valueArg->value();
  if (valueVector) {
    auto constantVec = valueVector->as<ConstantVector<int32_t>>();
    if (constantVec && !constantVec->isNullAt(0)) {
      value = constantVec->valueAt(0);
    }
  }
  
  // Get N argument (count) - also INTEGER type (int32_t)
  auto countArg = std::dynamic_pointer_cast<ScalarArgument>(args.at("N"));
  auto countVector = countArg->value();
  VELOX_CHECK_NOT_NULL(countVector, "count value for function constant() is null");
  int32_t count = countVector->as<ConstantVector<int32_t>>()->valueAt(0);
  
  VELOX_CHECK_GT(count, 0, "count value for function constant() must be positive");
  
  auto analysis = std::make_unique<ConstantFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<ConstantFunctionHandle>(value, count);
  return analysis;
}

void registerConstantFunction(const std::string& name) {
  // Source functions have no table arguments
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<ScalarArgumentSpecification>("VALUE", INTEGER(), false));
  argSpecs.insert(
      std::make_shared<ScalarArgumentSpecification>("N", INTEGER(), false, "1"));
  
  // Create descriptor for the return type: single INTEGER column named "constant_column"
  std::vector<std::string> returnNames = {"constant_column"};
  std::vector<TypePtr> returnTypes = {INTEGER()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      ConstantFunction::analyze,
      TableFunction::defaultCreateDataProcessor,  // No data processor
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionSplitProcessor> {
        return std::make_unique<ConstantFunctionSplitProcessor>(
            dynamic_cast<const ConstantFunctionHandle*>(handle.get()), pool);
      },
      ConstantFunction::getSplits);
}

// TestSingleInputFunction implementation
TestSingleInputFunctionDataProcessor::TestSingleInputFunctionDataProcessor(
    const TestSingleInputFunctionHandle* handle,
    memory::MemoryPool* pool)
    : TableFunctionDataProcessor("test_single_input", pool, nullptr),
      handle_(handle) {
  // Pre-build the result Page once (matching Java behavior)
  // The Java implementation creates the result Page once in the processor provider
  // and reuses it for every input
  auto boolColumn = BaseVector::create<FlatVector<bool>>(BOOLEAN(), 1, pool);
  auto* rawBools = boolColumn->mutableRawValues();
  rawBools[0] = true;
  
  auto rowType = ROW({BOOLEAN()});
  std::vector<VectorPtr> children = {boolColumn};
  result_ = std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), 1, std::move(children));
}

std::shared_ptr<TableFunctionResult> TestSingleInputFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  
  // Handle null input - return FINISHED
  if (!inputTable) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  
  // Return the pre-built result (same Page for every input)
  // This matches the Java behavior where the same Page is reused
  return std::make_shared<TableFunctionResult>(true, result_);
}

std::unique_ptr<TestSingleInputFunctionAnalysis> TestSingleInputFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));
  
  // Require all input columns (per Java implementation)
  std::vector<column_index_t> requiredColsList;
  for (size_t i = 0; i < input->rowType()->size(); i++) {
    requiredColsList.push_back(i);
  }
  RequiredColumnsMap requiredColumns;
  requiredColumns.emplace("INPUT", requiredColsList);
  
  auto analysis = std::make_unique<TestSingleInputFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<TestSingleInputFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerTestSingleInputFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", true, true, false));  // rowSemantics=true, pruneWhenEmpty=true, passThroughColumns=false
  
  // Create descriptor for the return type: single BOOLEAN column named "boolean_result"
  std::vector<std::string> returnNames = {"boolean_result"};
  std::vector<TypePtr> returnTypes = {BOOLEAN()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      TestSingleInputFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<TestSingleInputFunctionDataProcessor>(
            dynamic_cast<const TestSingleInputFunctionHandle*>(handle.get()), pool);
      });
}


} // namespace facebook::presto::tvf

