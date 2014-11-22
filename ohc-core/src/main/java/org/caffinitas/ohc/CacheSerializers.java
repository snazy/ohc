package org.caffinitas.ohc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class CacheSerializers
{
    public static final CacheSerializer<String> stringSerializer = new CacheSerializer<String>()
    {
        public void serialize(String s, OutputStream out) throws IOException
        {
            out.write(s.getBytes());
        }

        public String deserialize(InputStream in) throws IOException
        {
            byte[] b = new byte[in.available()];
            in.read(b);
            return new String(b);
        }

        public long serializedSize(String s)
        {
            return s.getBytes().length;
        }
    };

    public static final CacheSerializer<byte[]> byteArraySerializer = new CacheSerializer<byte[]>()
    {
        public void serialize(byte[] s, OutputStream out) throws IOException
        {
            out.write(s);
        }

        public byte[] deserialize(InputStream in) throws IOException
        {
            byte[] b = new byte[in.available()];
            in.read(b);
            return b;
        }

        public long serializedSize(byte[] s)
        {
            return s.length;
        }
    };
}
