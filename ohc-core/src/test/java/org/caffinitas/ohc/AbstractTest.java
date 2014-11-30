package org.caffinitas.ohc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

        public int serializedSize(String s)
        {
            return AbstractDataOutput.writeUTFLen(s);
        }
    };

    protected OHCacheBuilder<Object, Object> newBuilder()
    {
        return OHCacheBuilder.newBuilder();
    }

}
