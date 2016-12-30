package org.caffinitas.ohc.chunked;

import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

/**
 * Created by chaoweizhchw on 2016/12/30.
 */
public class SizeTest {

    @Test
    public void testSize() throws InterruptedException {
        ByteArrayCacheSerializer serializer = new ByteArrayCacheSerializer();
        OHCache ohCache = OHCacheBuilder.<byte[], byte[]>newBuilder().capacity(1024*4).throwOOME(true)
                .keySerializer(serializer).valueSerializer(serializer).segmentCount(1).chunkSize(1024).hashTableSize(256)
                .build();

        for(int i=0;i<12;++i){
            byte[] key = longToBytes(i);
            byte[] value = new byte[256];
            ohCache.put(key,value);
            System.out.println(i);
            System.out.println(ohCache.stats());
            ohCache.remove(key);
            System.out.println(ohCache.stats());
        }

        for(int i=12;i<16;++i){
            byte[] key = longToBytes(i);
            byte[] value = new byte[256];
            ohCache.put(key,value);
            System.out.println(ohCache.stats());
        }
    }


    private byte[]  longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    static private class ByteArrayCacheSerializer implements CacheSerializer<byte[]> {
        @Override
        public void serialize(byte[] value, ByteBuffer buf) {
            buf.put(value);
        }
        @Override
        public byte[] deserialize(ByteBuffer buf) {
            byte[] bytes = new byte[buf.capacity()];
            buf.get(bytes);
            return bytes;
        }
        @Override
        public int serializedSize(byte[] value) {
            return value.length;
        }
    }
}
