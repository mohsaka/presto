package com.facebook.presto.metadata;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;

import javax.inject.Inject;

import java.util.function.Function;

public class TableFunctionJacksonHandleModule
        extends AbstractTypedJacksonModule<ConnectorTableFunctionHandle>
{
    @Inject
    public TableFunctionJacksonHandleModule(HandleResolver handleResolver)
    {
        super(ConnectorTableFunctionHandle.class,
                handleResolver::getId,
                handleResolver::getTableFunctionHandleClass);
    }
}