package org.caffinitas.ohc;

public abstract class AbstractTest
{

    protected OHCache<Object, Object> nonEvicting()
    {
        return newBuilder()
               .cleanUpTriggerMinFree(0L)
               .build();
    }

    protected OHCacheBuilder<Object, Object> newBuilder()
    {
        return OHCacheBuilder.newBuilder()
                             .dataManagement(DataManagement.FIXED_BLOCKS);
    }

    protected abstract boolean withPartitionReadWriteLocks();

}
