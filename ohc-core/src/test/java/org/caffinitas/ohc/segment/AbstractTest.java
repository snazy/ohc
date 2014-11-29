package org.caffinitas.ohc.segment;

import org.caffinitas.ohc.api.OHCache;
import org.caffinitas.ohc.api.OHCacheBuilder;

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
        return OHCacheBuilder.newBuilder();
    }

}
