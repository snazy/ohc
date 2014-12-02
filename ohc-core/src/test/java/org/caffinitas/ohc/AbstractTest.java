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
            return writeUTFLen(s);
        }
    };

    public static int writeUTFLen(String str)
    {
        int strlen = str.length();
        int utflen = 0;
        int c;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++)
        {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }

        if (utflen > 65535)
            throw new RuntimeException("encoded string too long: " + utflen + " bytes");

        return utflen + 2;
    }
}
