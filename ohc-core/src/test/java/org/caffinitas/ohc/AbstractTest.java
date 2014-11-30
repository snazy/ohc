package org.caffinitas.ohc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.caffinitas.ohc.AbstractDataOutput;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

public abstract class AbstractTest
{

    public static final CacheSerializer<String> stringSerializer = new CacheSerializer<String>()
    {
        public void serialize(String s, DataOutput out) throws IOException
        {
            out.writeUTF(s);
        }

        public String deserialize(DataInput in) throws IOException
        {
            return in.readUTF();
        }

        public long serializedSize(String s)
        {
            return AbstractDataOutput.writeUTFLen(s);
        }
    };

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
