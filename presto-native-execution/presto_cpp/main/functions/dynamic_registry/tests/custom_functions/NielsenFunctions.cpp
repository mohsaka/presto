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
#include <folly/Portability.h>
#include <iostream>
#include <stdexcept>
#include <string.h>
#include "presto_cpp/main/functions/dynamic_registry/DynamicFunctionRegistrar.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/type/Type.h"
 // This file defines a function that will be dynamically linked and
 // registered. This function implements a custom function in the definition,
 // and this library (.so) needs to provide a `void registerExtensions()`
 // C function in the top-level namespace.
 //
 // (note the extern "C" directive to prevent the compiler from mangling the
 // symbol name).

namespace ibm::nielsen {
constexpr auto kMaxDemos = 2;
constexpr auto kMinStorePercent = 0.0001;
constexpr auto kMaxStorePercent = 0.9999;
constexpr auto kDemoCount = 2;

namespace {
struct StoreFact {
  double strPct;
  double pdi;
  double saaf;
  double sdr;
  double remSdr;
  double shrRemSdr;
  double shrRemOther;
  double sdrDemo;
  double sdrDemoRem;
  double finalRemSdr;
  StoreFact() {
    strPct = 0.0000;
    pdi = 0.0000;
    saaf = 0.0000;
    sdr = 0.0000;
    remSdr = 0.0000;
    shrRemSdr = 0.0000;
    shrRemOther = 0.0000;
    sdrDemo = 0.0000;
    sdrDemoRem = 0.0000;
    finalRemSdr = 0.0000;
  }
};
inline bool isNaN(double x) {
  return x != x;
}
} // namespace

double cTargettrackDemoEval(
    double argHsStrPct,
    double argHsPdi,
    double argHsSaaf,
    double argAfamStrPct,
    double argAfamPdi,
    double argAfamSaaf,
    int32_t argIndex) {
  std::vector<double> inputs = {argHsStrPct, argHsPdi, argHsSaaf, argAfamStrPct, argAfamPdi, argAfamSaaf};
  int i = 0, j = 0;
  std::vector<StoreFact> storeFactArr(kMaxDemos + 1);
  StoreFact storeFacttemp;
  double denom = 0.0;
  double sumFinalRemSdr = 0.0;
  double retVal = 0.0;
  if (argIndex == -1) {
    return 1.0;
  }
  for (i = 0; i <= 3 * kDemoCount; ++i) {
    if (inputs.size() < i) {
      return 0.0;
    }
  }

  for (i = 0; i < kDemoCount; ++i) {
    if (inputs.at(3 * i) == -1.0) {
      return std::numeric_limits<double>::quiet_NaN();
    }
  }

  if (argIndex < 0 || (argIndex > kDemoCount && argIndex <= 10) ||
      argIndex > 10 + kDemoCount) {
    return 0.0;
  }
  memset(storeFactArr.data(), 0, sizeof(storeFactArr));
  memset(&storeFacttemp, 0, sizeof(storeFacttemp));
  for (i = 0; i < kDemoCount; i++) {
    storeFactArr[i].strPct = inputs.at(3 * i + 0);
    // Adjust the store percent to be within allowable range
    if (storeFactArr[i].strPct < 0.0) {
      storeFactArr[i].strPct = kMinStorePercent;
    } else if (storeFactArr[i].strPct >= 1.0) {
      storeFactArr[i].strPct = kMaxStorePercent;
    }
    sumFinalRemSdr +=
        storeFactArr[i].strPct; // Reusing this variable for total store percent
    storeFactArr[i].pdi = inputs.at(3 * i + 1);
    storeFactArr[i].saaf = inputs.at(3 * i + 2);
  }
  // If total store percentage is > 100% adjust
  if (sumFinalRemSdr > 1.0) {
    for (i = 0; i < kDemoCount; i++) {
      storeFactArr[i].strPct /= sumFinalRemSdr;
    }
  }
  for (i = 0; i < kDemoCount; i++) {
    if (i != 0) // swap the i-th record with the 0-th record.
    {
      storeFacttemp = storeFactArr[0];
      storeFactArr[0] = storeFactArr[i];
      storeFactArr[i] = storeFacttemp;
    }
    for (j = 0; j < kDemoCount; j++) {
      if (j == 0) {
        storeFactArr[j].sdr = (storeFactArr[j].strPct * storeFactArr[j].pdi) /
            ((storeFactArr[j].strPct * storeFactArr[j].pdi) +
             (1 - storeFactArr[j].strPct));
        storeFactArr[j].remSdr = 1 - storeFactArr[j].sdr;
        storeFactArr[j].shrRemSdr = 0;
        storeFactArr[j].shrRemOther = 0;
        storeFactArr[j].sdrDemo = 0;
        storeFactArr[j].sdrDemoRem = 0;
        denom = 1;
      } else {
        denom = denom - storeFactArr[j - 1].strPct;
        storeFactArr[j].remSdr =
            storeFactArr[j - 1].remSdr - storeFactArr[j - 1].sdrDemoRem;
        storeFactArr[j].shrRemSdr =
            (storeFactArr[j].remSdr * storeFactArr[j].strPct) / (denom);
        storeFactArr[j].shrRemOther =
            storeFactArr[j].remSdr - storeFactArr[j].shrRemSdr;
        storeFactArr[j].sdrDemo =
            (storeFactArr[j].shrRemSdr * storeFactArr[j].pdi) /
            ((storeFactArr[j].shrRemSdr * storeFactArr[j].pdi) +
             storeFactArr[j].shrRemOther);
        storeFactArr[j].sdrDemoRem =
            storeFactArr[j].sdrDemo * storeFactArr[j].remSdr;
      }
    } // end of inner for loop (j)
    storeFactArr[0].finalRemSdr =
        storeFactArr[j - 1].remSdr - storeFactArr[j - 1].sdrDemoRem;
  } // end of outer for loop (i);
  // Initially the arg0,arg1,arg2 and arg3 ... argn were in the array in order
  // (0,1,2,3 ...n) Now they are in the order n,0,1,2...n-1 Now we are going to
  // put it in the correct order.
  storeFacttemp = storeFactArr[0];
  for (i = 0; i < kDemoCount - 1; i++) {
    storeFactArr[i] = storeFactArr[i + 1];
  }
  storeFactArr[i] = storeFacttemp;
  sumFinalRemSdr = 0;
  j = 0;

  for (i = kDemoCount - 1; i >= 0; i--) {
    if (storeFactArr[i].strPct > 0 && j == 0) {
      j = i + 1;
    }
    if (j > 0) {
      sumFinalRemSdr += storeFactArr[i].finalRemSdr;
    }
  }
  sumFinalRemSdr = (sumFinalRemSdr / (double)j);
  for (i = 0; i < kDemoCount; i++) {
    sumFinalRemSdr += storeFactArr[i].sdr;
  }
  // SDRs for first balancing
  denom = 0; // re-using the variable to reduce the stack size.
  for (i = 0; i < kDemoCount; i++) {
    storeFactArr[i].sdr = storeFactArr[i].sdr / sumFinalRemSdr;
    denom += storeFactArr[i].sdr;
  }
  sumFinalRemSdr = 1 - denom;

  // SDRs ties Demo-SAAF
  denom = 0; // re-using the variable to reduce the stack size.
  for (i = 0; i < kDemoCount; i++) {
    storeFactArr[i].sdr = storeFactArr[i].sdr * storeFactArr[i].saaf;
    denom += storeFactArr[i].sdr;
  }
  // SDRs for second balancing
  sumFinalRemSdr = 0;
  for (i = 0; i < kDemoCount; i++) {
    if (denom > 1) {
      storeFactArr[i].sdr = storeFactArr[i].sdr / denom;
    }
    sumFinalRemSdr += storeFactArr[i].sdr;
  }
  sumFinalRemSdr = 1 - sumFinalRemSdr;
  if (argIndex == 0) {
    retVal = sumFinalRemSdr; // other demo.
  } else if (argIndex <= kDemoCount) {
    retVal = storeFactArr[argIndex - 1].sdr;
  } else {
    retVal = 1 - storeFactArr[argIndex - 10 - 1].sdr;
  }
  retVal = std::floor(retVal * 1000000 + 0.5) / 1000000;
  if (isNaN(retVal)) {
    retVal = 0;
  }
  return retVal; // Return the calculated value
}

/// First check if either market restriction position index or product exclusion char or product restriction index is 0.
///   - If yes, return 1 and DONE!
///   - If no, do the BITAND between market restriction position index and product exclusion char.
///   - If the result is 0 return 1 from the UDF and DONE! Basically no need to perform the 2nd BITAND with the product restriction index anymore.
///   - If the result is greater than or equal to 1, perform 2nd BITAND between the result from 1st BITAND and product restriction index.
///     - If result is greater than or equal to 1 return NULL else 1.
/// Arguments: mktrstr = MRKT_DIM.MRKT_RSTR_POS. prdcexclchr = Product exclusion char derived. prdcrstr = PRDC_DIM.RSTR_IND.
int32_t xdimRstrUDFEval(int32_t mktrstr, int32_t prdcexclchr, int32_t prdcrstr) {
  try {
    // Check if any input is non-positive
    if (mktrstr <= 0 || prdcexclchr <= 0 || prdcrstr <= 0) {
      return 1; // Return 1 as specified, if any input is zero or negative
    }

    // First bitwise AND operation
    int result1 = mktrstr & prdcexclchr;
    if (result1 == 0) {
      return 1; // Return 1 if the result of the first AND operation is 0
    }

    // Second bitwise AND operation
    int result2 = result1 & prdcrstr;
    if (result2 == 0) {
      return 1; // Return 1 if the result of the second AND operation is 0
    }

    // If the second result is non-zero, return 0 as a placeholder for null
    if (result2 >= 0) {
      return 0;
    }
  } catch (...) {
    throw std::invalid_argument(
        "XDIM_RSTR_UDF: Unexpected column value/combination");
  }
  VELOX_UNREACHABLE("Unexpected code path in xdimRstrUDFEval");
}

template <typename T>
struct XdimRstrUdfDemo {
      VELOX_DEFINE_FUNCTION_TYPES(T);
      void call(
          out_type<int32_t>& result,
          const arg_type<int32_t>& mktrstr,
          const arg_type<int32_t>& prdcexclchr,
          const arg_type<int32_t>& prdcrstr) {
        result = xdimRstrUDFEval(mktrstr, prdcexclchr, prdcrstr);
      }
};
template <typename T>
struct CTargettrackDemo {
      VELOX_DEFINE_FUNCTION_TYPES(T);
      FOLLY_ALWAYS_INLINE void call(
          out_type<double>& result,
          const arg_type<double>& argHsStrPct,
          const arg_type<double>& argHsPdi,
          const arg_type<double>& argHsSaaf,
          const arg_type<double>& argAfamStrPct,
          const arg_type<double>& argAfamPdi,
          const arg_type<double>& argAfamSaaf,
          const arg_type<int32_t>& value7) {
        result = cTargettrackDemoEval(
            argHsStrPct, argHsPdi, argHsSaaf, argAfamStrPct, argAfamPdi, argAfamSaaf, value7);
      }
};
} // namespace custom::functionRegistry

extern "C" {
    void registerExtensions() {
       facebook::presto::registerPrestoFunction<ibm::nielsen::XdimRstrUdfDemo, int32_t, int32_t, int32_t, int32_t>(
            "xdim_rstr_udf");
       facebook::presto::registerPrestoFunction<
            ibm::nielsen::CTargettrackDemo,
            double,
            double,
            double,
            double,
            double,
            double,
            double,
            int32_t>("targettrack_demo");
    }
}
