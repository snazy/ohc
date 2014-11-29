package org.caffinitas.ohc.mono;

import org.caffinitas.ohc.api.OHCache;
import org.caffinitas.ohc.api.OHCacheBuilder;

public abstract class AbstractTest
{

    protected OHCache<Object, Object> nonEvicting()
    {
        return newBuilder()
               .cleanUpTriggerMinFree(0L)
               .monoExperimental(true)
               .build();
    }

    protected OHCacheBuilder<Object, Object> newBuilder()
    {
        return OHCacheBuilder.newBuilder();
    }

}
