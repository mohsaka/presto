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
package com.facebook.presto.metadata;

import com.facebook.presto.common.type.TypeSignature;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import com.facebook.presto.metadata.CatalogServiceProvider;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.table.SchemaFunctionName;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UnknownType;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TableFunctionRegistry
{
    private final CatalogServiceProvider<CatalogTableFunctions> tableFunctionsProvider;

    @Inject
    public TableFunctionRegistry(CatalogServiceProvider<CatalogTableFunctions> tableFunctionsProvider)
    {
        this.tableFunctionsProvider = requireNonNull(tableFunctionsProvider, "tableFunctionsProvider is null");
    }

    public List<FunctionMetadata> listTableFunctions(ConnectorId connectorId)
    {
        return tableFunctionsProvider.getService(connectorId).listTableFunctions().values().stream()
                .map(TableFunctionRegistry::toFunctionMetadata)
                .collect(toImmutableList());
    }

    public List<FunctionMetadata> listTableFunctions(ConnectorId connectorId, String schemaName)
    {
        return tableFunctionsProvider.getService(connectorId).listTableFunctions().values().stream()
                .filter(function -> function.getSchema().equals(schemaName))
                .map(TableFunctionRegistry::toFunctionMetadata)
                .collect(toImmutableList());
    }

    /**
     * Resolve table function with given qualified name.
     * Table functions are resolved case-insensitive for consistency with existing scalar function resolution.
     */
    public Optional<ConnectorTableFunction> resolve(ConnectorId connectorId, SchemaFunctionName schemaFunctionName)
    {
        return tableFunctionsProvider.getService(connectorId)
                .getTableFunction(schemaFunctionName);
    }

    private static FunctionMetadata toFunctionMetadata(ConnectorTableFunction tableFunction)
    {
        return FunctionMetadata.tableBuilder(tableFunction.getName())
                .signature(Signature.builder()
                        .argumentTypes(toArgumentTypes(tableFunction))
                        .returnType(UnknownType.UNKNOWN)
                        .build())
                .noDescription()
                .nondeterministic()
                .build();
    }

    private static List<TypeSignature> toArgumentTypes(ConnectorTableFunction tableFunction)
    {
        return tableFunction.getArguments().stream()
                .map(function -> {
                    if (function instanceof ScalarArgumentSpecification scalarArgument) {
                        return scalarArgument.getType().getTypeSignature();
                    }
                    return UnknownType.UNKNOWN.getTypeSignature();
                })
                .collect(toImmutableList());
    }
}
