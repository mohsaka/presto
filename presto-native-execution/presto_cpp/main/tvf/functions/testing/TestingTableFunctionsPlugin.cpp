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

#include "presto_cpp/main/tvf/functions/testing/TestingTableFunctions.h"

// This file defines the plugin entry point for dynamically loading testing
// table functions. The library (.so/.dylib) provides a `void registerExtensions()`
// C function in the top-level namespace that will be called when the plugin
// is loaded.
//
// (note the extern "C" directive to prevent the compiler from mangling the
// symbol name).

extern "C" {
// The function registerExtensions is the entry point to execute the
// registration of the table functions and cannot be changed.
void registerExtensions() {
  facebook::presto::tvf::registerRepeatFunction("repeat_table_function");
  facebook::presto::tvf::registerIdentityFunction("identity_table_function");
  facebook::presto::tvf::registerSimpleTableFunction("simple_table_function");
}
}

// Made with Bob
