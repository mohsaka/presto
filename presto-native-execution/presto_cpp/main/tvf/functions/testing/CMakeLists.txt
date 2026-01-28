# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Static library for tests that need to link against testing functions
add_library(
  presto_tvf_testing_functions STATIC
  TestingTableFunctions.cpp
)

target_link_libraries(presto_tvf_testing_functions presto_tvf_spi)

# Shared library plugin for dynamic loading at runtime
add_library(
  presto_tvf_testing_plugin SHARED
  TestingTableFunctions.cpp
  TestingTableFunctionsPlugin.cpp
)

target_link_libraries(presto_tvf_testing_plugin presto_tvf_spi)