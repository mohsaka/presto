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

#include "presto_cpp/main/tvf/exec/TableFunctionOperator.h"

#include "presto_cpp/main/tvf/exec/TableFunctionPartition.h"

#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

const std::vector<RowTypePtr> requiredColumnTypes(
    const TableFunctionProcessorNodePtr& tableFunctionProcessorNode) {
  auto columns = tableFunctionProcessorNode->requiredColumns();
  auto inputType = tableFunctionProcessorNode->sources()[0]->outputType();
  std::vector<RowTypePtr> result;
  result.reserve(columns.size());
  for (const auto& sourceColumns : columns) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (const auto& idx : sourceColumns) {
      names.push_back(inputType->nameOf(idx));
      types.push_back(inputType->childAt(idx));
    }
    result.push_back(ROW(std::move(names), std::move(types)));
  }
  return result;
}
} // namespace

TableFunctionOperator::TableFunctionOperator(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const TableFunctionProcessorNodePtr& tableFunctionProcessorNode)
    : Operator(
          driverCtx,
          tableFunctionProcessorNode->outputType(),
          operatorId,
          tableFunctionProcessorNode->id(),
          "TableFunctionOperator",
          tableFunctionProcessorNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      pool_(pool()),
      stringAllocator_(pool_),
      tableFunctionProcessorNode_(tableFunctionProcessorNode),
      inputType_(tableFunctionProcessorNode->sources()[0]->outputType()),
      requiredColumnTypes_(requiredColumnTypes(tableFunctionProcessorNode)),
      tableFunctionPartition_(nullptr),
      functionInput_({}),
      future_(nullptr) {
  tablePartitionBuild_ = std::make_unique<TablePartitionBuild>(
      tableFunctionProcessorNode,
      requiredColumnTypes_,
      pool(),
      common::PrefixSortConfig{
          driverCtx->queryConfig().prefixSortNormalizedKeyMaxBytes(),
          driverCtx->queryConfig().prefixSortMinRows(),
          driverCtx->queryConfig().prefixSortMaxStringPrefixLength()});
  numRowsPerOutput_ = outputBatchRows(tablePartitionBuild_->estimateRowSize());
}

void TableFunctionOperator::initialize() {
  Operator::initialize();
  VELOX_CHECK_NOT_NULL(tableFunctionProcessorNode_);
}

void TableFunctionOperator::createTableFunctionDataProcessor(
    const std::shared_ptr<const TableFunctionProcessorNode>& node) {
  dataProcessor_ = TableFunction::createDataProcessor(
      node->functionName(),
      node->handle(),
      pool_,
      &stringAllocator_,
      operatorCtx_->driverCtx()->queryConfig());
  VELOX_CHECK(dataProcessor_);
}

// Writing the code to add the input rows -> call TableFunction::process and
// return the rows from it. This is done per input vectors basis. If we have
// partition by an order by this would need a change but just testing with a
// simple model for now.
void TableFunctionOperator::addInput(RowVectorPtr input) {
  numRows_ += input->size();

  tablePartitionBuild_->addInput(input);
}

void TableFunctionOperator::noMoreInput() {
  Operator::noMoreInput();
  tablePartitionBuild_->noMoreInput();
}

RowVectorPtr TableFunctionOperator::getOutputFromFunction() {
  VELOX_CHECK(tableFunctionPartition_);
  VELOX_CHECK(dataProcessor_);

  // This is the first call to TableFunction::apply for this partition
  // or a previous apply for this input has completed.
  if (!validFunctionInput_) {
    // Check if all rows have been processed and we need to signal end-of-stream
    bool allRowsProcessed = (tableFunctionPartition_->numRows() - numPartitionProcessedRows_) == 0;
    // Only functions with multiple table inputs need the nullptr call to coordinate end-of-stream
    bool hasMultipleInputs = requiredColumnTypes_.size() > 1;
    
    if (allRowsProcessed && !calledWithNullptr_ && hasMultipleInputs) {
      // Signal end-of-stream by passing nullptr for all inputs
      // Don't call assembleInput() as it will fail the VELOX_CHECK_GT(numRowsLeft, 0)
      functionInput_.clear();
      for (size_t i = 0; i < requiredColumnTypes_.size(); i++) {
        functionInput_.push_back(nullptr);
      }
      calledWithNullptr_ = true;
    } else if (!allRowsProcessed) {
      // Only call assembleInput if there are rows left to process
      functionInput_ = tableFunctionPartition_->assembleInput(
          numRowsPerOutput_, numPartitionProcessedRows_);
    } else {
      // All rows processed and (already called with nullptr for multiple inputs OR single input function)
      return nullptr;
    }
    validFunctionInput_ = true;
  }
  
  auto result = dataProcessor_->apply(functionInput_);
  
  if (result->state() == TableFunctionResult::TableFunctionState::kFinished) {
    // Skip the rest of this partition processing.
    numProcessedRows_ +=
        (tableFunctionPartition_->numRows() - numPartitionProcessedRows_);
    tableFunctionPartition_ = nullptr;
    numPartitionProcessedRows_ = 0;
    validFunctionInput_ = false;
    // Mark that we've finished the empty partition if numRows_ == 0
    if (numRows_ == 0) {
      finishedEmptyPartition_ = true;
    }
    // Don't reset calledWithNullptr_ here - we need it for isFinished() check
    return nullptr;
  }

  if (result->state() == TableFunctionResult::TableFunctionState::kBlocked) {
    future_ = std::move(result->future());
    return nullptr;
  }

  VELOX_CHECK(
      result->state() == TableFunctionResult::TableFunctionState::kProcessed);
  auto resultRows = result->result();
  
  // Handle nullptr result (empty partition case)
  if (!resultRows) {
    if (result->usedInput()) {
      // The input rows were consumed, so we need to re-assemble input at the
      // next call. Count rows from all inputs, not just the first one.
      velox::vector_size_t totalInputRows = 0;
      for (const auto& input : functionInput_) {
        if (input) {
          totalInputRows = std::max(totalInputRows, input->size());
        }
      }
      numPartitionProcessedRows_ += totalInputRows;
      numProcessedRows_ += totalInputRows;
      validFunctionInput_ = false;
    }
    return nullptr;
  }
  
  if (result->usedInput()) {
    // The input rows were consumed, so we need to re-assemble input at the
    // next call. Count rows from all inputs, not just the first one.
    velox::vector_size_t totalInputRows = 0;
    for (const auto& input : functionInput_) {
      if (input) {
        totalInputRows = std::max(totalInputRows, input->size());
      }
    }
    numPartitionProcessedRows_ += totalInputRows;
    numProcessedRows_ += totalInputRows;
  }

  // Append passthrough columns if any.
  auto finalResult = tableFunctionPartition_->appendPassThroughColumns(resultRows);
  
  // Mark input as consumed only if the function actually used it
  // If usedInput() is false, keep the same input for the next call
  if (result->usedInput()) {
    validFunctionInput_ = false;
  }
  
  // Operator::getOutput() must return nullptr or a non-empty vector
  // If the result is empty, return nullptr instead
  if (finalResult && finalResult->size() == 0) {
    return nullptr;
  }
  
  return std::move(finalResult);
}

RowVectorPtr TableFunctionOperator::getOutput() {
  if (!noMoreInput_) {
    return nullptr;
  }

  auto initNewPartition = [&]() -> void {
    createTableFunctionDataProcessor(tableFunctionProcessorNode_);
    numPartitionProcessedRows_ = 0;
    validFunctionInput_ = false;
    calledWithNullptr_ = false;
    finishedEmptyPartition_ = false;
  };

  auto noRemainingInputForPartition = [&]() -> bool {
    bool hasPartition = tableFunctionPartition_ != nullptr;
    bool allRowsProcessed = hasPartition &&
        (tableFunctionPartition_->numRows() - numPartitionProcessedRows_ == 0);
    // Only multiple inputs need the nullptr call
    bool hasMultipleInputs = requiredColumnTypes_.size() > 1;
    // IMPORTANT: Don't consider partition done if validFunctionInput_ is true
    // This means the function has output to produce but hasn't consumed the input yet
    return hasPartition && allRowsProcessed && (!hasMultipleInputs || calledWithNullptr_) && !validFunctionInput_;
  };

  // Setup partition if needed.
  if (numRows_ == 0) {
    if (tableFunctionProcessorNode_->pruneWhenEmpty()) {
      return nullptr;
    } else if (finishedEmptyPartition_) {
      return nullptr;
    } else {
      // This function has not received any input rows but processes empty
      // input. Only initialize if we haven't already.
      if (tableFunctionPartition_ == nullptr) {
        tableFunctionPartition_ = tablePartitionBuild_->emptyPartition();
        initNewPartition();
      }
    }

  } else {
    // Its enough to check only numProcessedRows_ as this is incremented only
    // after the function has signalled it processed the input rows.
    const auto numRowsLeft = numRows_ - numProcessedRows_;
    
    if (numRowsLeft == 0) {
      bool noRemaining = noRemainingInputForPartition();
      
      if (noRemaining) {
        return nullptr;
      }
    }

    // There is no partition being processed or the previous partition has been
    // fully processed (there is no unprocessed input and nothing left in the
    // partition either).
    bool noRemaining = noRemainingInputForPartition();
    
    if (tableFunctionPartition_ == nullptr || noRemaining) {
      if (tablePartitionBuild_->hasNextPartition()) {
        tableFunctionPartition_ = tablePartitionBuild_->nextPartition();
        initNewPartition();
      } else {
        return nullptr;
      }
    }
  }

  return getOutputFromFunction();
}

void TableFunctionOperator::reclaim(
    uint64_t /*targetBytes*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_NYI("TableFunctionOperator::reclaim not implemented");
}

} // namespace facebook::presto::tvf
