package org.caffinitas.ohc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class CacheSerializers
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

    public static final CacheSerializer<byte[]> byteArraySerializer = new CacheSerializer<byte[]>()
    {
        public void serialize(byte[] s, DataOutput out) throws IOException
        {
            out.write(s.length);
            out.write(s);
        }

        public byte[] deserialize(DataInput in) throws IOException
        {
            int l = in.readInt();
            byte[] b = new byte[l];
            in.readFully(b);
            return b;
        }

        public long serializedSize(byte[] s)
        {
            return s.length;
        }
    };
}
