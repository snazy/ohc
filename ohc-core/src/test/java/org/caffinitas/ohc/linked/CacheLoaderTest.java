package org.caffinitas.ohc.linked;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.caffinitas.ohc.CacheLoader;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.PermanentLoadException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CacheLoaderTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static int loaderCalled;
    static int loaderPermFailCalled;
    static int slowLoaderCalled;
    static int slowLoaderTempFailCalled;
    static int slowLoaderPermFailCalled;

    static final CacheLoader<Integer, String> loader = new CacheLoader<Integer, String>()
    {
        public String load(Integer key) throws Exception
        {
            loaderCalled++;
            return key.toString();
        }
    };
    static final CacheLoader<Integer, String> loaderNull = new CacheLoader<Integer, String>()
    {
        public String load(Integer key) throws Exception
        {
            loaderCalled++;
            return null;
        }
    };
    static final CacheLoader<Integer, String> loaderTempFail = new CacheLoader<Integer, String>()
    {
        public String load(Integer key) throws Exception
        {
            throw new Exception("foo");
        }
    };
    static final CacheLoader<Integer, String> loaderPermFail = new CacheLoader<Integer, String>()
    {
        public String load(Integer key) throws Exception
        {
            loaderPermFailCalled++;
            throw new PermanentLoadException("bar");
        }
    };

    static final CacheLoader<Integer, String> slowLoader = new CacheLoader<Integer, String>()
    {
        public String load(Integer key) throws Exception
        {
            slowLoaderCalled++;
            Thread.sleep(500);
            return key.toString();
        }
    };
    static final CacheLoader<Integer, String> slowLoaderTempFail = new CacheLoader<Integer, String>()
    {
        public String load(Integer key) throws Exception
        {
            slowLoaderTempFailCalled++;
            Thread.sleep(500);
            throw new Exception("foo");
        }
    };
    static final CacheLoader<Integer, String> slowLoaderPermFail = new CacheLoader<Integer, String>()
    {
        public String load(Integer key) throws Exception
        {
            slowLoaderPermFailCalled++;
            Thread.sleep(500);
            throw new PermanentLoadException("bar");
        }
    };

    @DataProvider(name = "types")
    public Object[][] cacheEviction()
    {
        return new Object[][]{ { Eviction.LRU }, { Eviction.W_TINY_LFU }, { Eviction.NONE} };
    }

    @Test(dataProvider = "types")
    public void testGetWithLoaderAsync(Eviction eviction) throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
        try
        {


            try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                  .keySerializer(TestUtils.intSerializer)
                                                  .valueSerializer(TestUtils.stringSerializer)
                                                  .executorService(executorService)
                                                  .eviction(eviction)
                                                  .build())
            {
                Future<String> f1 = cache.getWithLoaderAsync(1, loader);
                Assert.assertEquals("1", f1.get(100, TimeUnit.MILLISECONDS));
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Test(dataProvider = "types")
    public void testGetWithLoaderAsyncNull(Eviction eviction) throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
        try
        {


            try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                  .keySerializer(TestUtils.intSerializer)
                                                  .valueSerializer(TestUtils.stringSerializer)
                                                  .executorService(executorService)
                                                  .eviction(eviction)
                                                  .build())
            {
                Future<String> f1 = cache.getWithLoaderAsync(1, loaderNull);
                Assert.assertNull(f1.get(100, TimeUnit.MILLISECONDS));
                Assert.assertFalse(cache.containsKey(1));
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Test(dataProvider = "types")
    public void testGetWithLoaderAsyncTempFail(Eviction eviction) throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
        try
        {

            try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                                .keySerializer(TestUtils.intSerializer)
                                                                .valueSerializer(TestUtils.stringSerializer)
                                                                .executorService(executorService)
                                                                .eviction(eviction)
                                                                .build())
            {
                Future<String> fTempFail = cache.getWithLoaderAsync(1, loaderTempFail);
                try
                {
                    fTempFail.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof Exception);
                }

                Future<String> f1 = cache.getWithLoaderAsync(1, loader);
                Assert.assertEquals("1", f1.get(100, TimeUnit.MILLISECONDS));
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Test(dataProvider = "types")
    public void testGetWithLoaderAsyncPermFail(Eviction eviction) throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
        try
        {

            try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                                .keySerializer(TestUtils.intSerializer)
                                                                .valueSerializer(TestUtils.stringSerializer)
                                                                .executorService(executorService)
                                                                .eviction(eviction)
                                                                .build())
            {
                loaderCalled = 0;
                loaderPermFailCalled = 0;

                Future<String> fTempFail = cache.getWithLoaderAsync(1, loaderPermFail);
                try
                {
                    fTempFail.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof PermanentLoadException);
                }

                Assert.assertEquals(loaderPermFailCalled, 1);

                fTempFail = cache.getWithLoaderAsync(1, loaderPermFail);
                try
                {
                    fTempFail.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof PermanentLoadException);
                }

                Assert.assertEquals(loaderPermFailCalled, 1);

                Future<String> f1 = cache.getWithLoaderAsync(1, loader);
                try
                {
                    f1.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof PermanentLoadException);
                }

                Assert.assertEquals(loaderCalled, 0);
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Test(dataProvider = "types")
    public void testGetWithSlowLoaderAsync(Eviction eviction) throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
        try
        {


            try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                                .keySerializer(TestUtils.intSerializer)
                                                                .valueSerializer(TestUtils.stringSerializer)
                                                                .executorService(executorService)
                                                                .eviction(eviction)
                                                                .build())
            {
                slowLoaderCalled = 0;

                Future<String> f1 = cache.getWithLoaderAsync(1, slowLoader);
                Thread.sleep(20);
                Future<String> f2 = cache.getWithLoaderAsync(1, slowLoader);
                Future<String> f3 = cache.getWithLoaderAsync(1, slowLoader);

                Thread.sleep(100);

                Assert.assertEquals(slowLoaderCalled, 1);

                Assert.assertEquals("1", f1.get(500, TimeUnit.MILLISECONDS));
                Assert.assertEquals("1", f2.get(500, TimeUnit.MILLISECONDS));
                Assert.assertEquals("1", f3.get(500, TimeUnit.MILLISECONDS));

                Assert.assertEquals(slowLoaderCalled, 1);
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Test(dataProvider = "types")
    public void testGetWithSlowTempFailLoaderAsync(Eviction eviction) throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
        try
        {


            try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                                .keySerializer(TestUtils.intSerializer)
                                                                .valueSerializer(TestUtils.stringSerializer)
                                                                .executorService(executorService)
                                                                .eviction(eviction)
                                                                .build())
            {
                slowLoaderCalled = 0;
                slowLoaderTempFailCalled = 0;

                Future<String> f1 = cache.getWithLoaderAsync(1, slowLoaderTempFail);
                Thread.sleep(20);
                Future<String> f2 = cache.getWithLoaderAsync(1, slowLoader);
                Future<String> f3 = cache.getWithLoaderAsync(1, slowLoaderTempFail);
                Future<String> f4 = cache.getWithLoaderAsync(1, slowLoader);

                Thread.sleep(100);

                Assert.assertEquals(slowLoaderTempFailCalled, 1);
                Assert.assertEquals(slowLoaderCalled, 0);

                try
                {
                    f1.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof Exception);
                }
                try
                {
                    f2.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof Exception);
                }
                try
                {
                    f3.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof Exception);
                }
                try
                {
                    f4.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof Exception);
                }

                Assert.assertEquals(slowLoaderTempFailCalled, 1);
                Assert.assertEquals(slowLoaderCalled, 0);
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Test(dataProvider = "types")
    public void testGetWithSlowPermFailLoaderAsync(Eviction eviction) throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
        try
        {


            try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                                .keySerializer(TestUtils.intSerializer)
                                                                .valueSerializer(TestUtils.stringSerializer)
                                                                .executorService(executorService)
                                                                .eviction(eviction)
                                                                .build())
            {
                slowLoaderCalled = 0;
                slowLoaderTempFailCalled = 0;
                slowLoaderPermFailCalled = 0;

                Future<String> f1 = cache.getWithLoaderAsync(1, slowLoaderPermFail);
                Thread.sleep(20);
                Future<String> f2 = cache.getWithLoaderAsync(1, slowLoader);
                Future<String> f3 = cache.getWithLoaderAsync(1, slowLoaderTempFail);
                Future<String> f4 = cache.getWithLoaderAsync(1, slowLoaderPermFail);

                Thread.sleep(100);

                Assert.assertEquals(slowLoaderPermFailCalled, 1);
                Assert.assertEquals(slowLoaderTempFailCalled, 0);
                Assert.assertEquals(slowLoaderCalled, 0);

                try
                {
                    f1.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof PermanentLoadException);
                }
                try
                {
                    f2.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof PermanentLoadException);
                }
                try
                {
                    f3.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof PermanentLoadException);
                }
                try
                {
                    f4.get(500, TimeUnit.MILLISECONDS);
                    Assert.fail();
                }
                catch (ExecutionException e)
                {
                    Assert.assertTrue(e.getCause() instanceof PermanentLoadException);
                }

                Assert.assertEquals(slowLoaderPermFailCalled, 1);
                Assert.assertEquals(slowLoaderTempFailCalled, 0);
                Assert.assertEquals(slowLoaderCalled, 0);
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

}
