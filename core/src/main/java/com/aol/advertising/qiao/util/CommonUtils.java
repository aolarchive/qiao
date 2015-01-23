/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2011 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   CommonUtil.java	
 * Description:
 * @author:     ytung
 * @version:    1.0
 *
 ****************************************************************************/

package com.aol.advertising.qiao.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import javax.net.ServerSocketFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.aol.advertising.qiao.config.ConfigConstants;
import com.aol.advertising.qiao.exception.InsufficientFileLengthException;

public class CommonUtils
{
    public static final String KRYO_NULL_STRING = "\0";
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private static Logger logger = LoggerFactory.getLogger(CommonUtils.class);
    private static SimpleDateFormat friendlyTimeFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss z");


    public static ThreadPoolTaskExecutor createFixedThreadPoolExecutor(
            int threadPoolSize)
    {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threadPoolSize);
        executor.setMaxPoolSize(threadPoolSize);
        executor.setQueueCapacity(0);
        executor.setRejectedExecutionHandler(new CallerRunsPolicy());
        return executor;
    }


    public static ThreadPoolTaskExecutor createThreadPoolExecutor(
            int corePoolSize, int maxPoolSize, int queueCapacity)
    {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setRejectedExecutionHandler(new CallerRunsPolicy());
        return executor;
    }


    public static ScheduledThreadPoolExecutor createScheduledThreadPoolExecutor(
            final int poolSz, final String threadName)
    {
        return new ScheduledThreadPoolExecutor(poolSz, new ThreadFactory()
        {
            private AtomicInteger threadNum = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r)
            {
                if (poolSz == 1)
                    return new Thread(r, threadName);
                else
                    return new Thread(r, threadName
                            + threadNum.incrementAndGet());
            }
        });
    }


    public static ScheduledExecutorService createScheduledExecutorService(
            final int poolSz, final String threadName)
    {
        return Executors.newScheduledThreadPool(poolSz, new ThreadFactory()
        {
            private AtomicInteger threadNum = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r)
            {
                if (poolSz == 1)
                    return new Thread(r, threadName);
                else
                    return new Thread(r, threadName
                            + threadNum.incrementAndGet());
            }
        });
    }


    public static ExecutorService createSingleThreadExecutor(
            final String threadName)
    {
        return Executors.newSingleThreadExecutor(new ThreadFactory()
        {

            @Override
            public Thread newThread(Runnable r)
            {
                return new Thread(r, CommonUtils.resolveThreadName(threadName));
            }
        });

    }


    public static ThreadPoolTaskScheduler createThreadPoolScheduler(int poolSz,
            String threadNamePrefix)
    {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(poolSz);
        if (threadNamePrefix != null)
            scheduler.setThreadNamePrefix(threadNamePrefix);
        return scheduler;
    }


    public static void shutdownAndAwaitTermination(ExecutorService pool)
    {
        shutdownAndAwaitTermination(pool, 10, TimeUnit.SECONDS);
    }


    public static void shutdownAndAwaitTermination(ExecutorService pool,
            int waitTime, TimeUnit timeUnit)
    {
        pool.shutdown(); // Disable new tasks from being submitted
        try
        {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(waitTime, timeUnit))
            {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(waitTime, timeUnit))
                    logger.warn("Executor did not terminate");
            }
        }
        catch (InterruptedException ie)
        {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
        }
    }


    public static boolean sleepQuietly(long msecs)
    {
        boolean ok = true;
        try
        {
            Thread.sleep(msecs);
        }
        catch (InterruptedException e)
        {
            ok = false;
        }

        return ok;
    }


    public static void sleepButInterruptable(long msecs)
            throws InterruptedException
    {
        Thread.sleep(msecs);
    }


    /**
     * Extends scheme to include 'classpath'.
     * 
     * @param uriString
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    public static URL uriToURL(String uriString) throws URISyntaxException,
            IOException
    {
        URI uri = new URI(uriString);
        String scheme = uri.getScheme();
        if (scheme == null)
            throw new URISyntaxException(uriString,
                    "Invalid URI syntax: missing scheme => " + uriString);

        if (scheme.equals("classpath"))
        {
            ClassPathResource res = new ClassPathResource(
                    uri.getSchemeSpecificPart());
            return res.getURL();
        }
        else
        {
            return uri.toURL();
        }
    }


    public static ServerSocket createServerSocket(int startPort, int endPort)
            throws BindException
    {
        ServerSocket ssoc = null;
        ServerSocketFactory factory = ServerSocketFactory.getDefault();

        int port = startPort;
        while (true)
        {
            try
            {
                ssoc = factory.createServerSocket(port);
                break;
            }
            catch (IOException e)
            {
                if (port >= endPort)
                    throw new BindException(
                            "No available port to bind to in range ["
                                    + startPort + " .. " + endPort + "]");
                port++;
            }
        }

        return ssoc;
    }


    public static String getFriendlyTimeString(Date date)
    {
        return friendlyTimeFormat.format(date);
    }


    public static String getFriendlyTimeString(long tm)
    {
        if (tm > 0)
            return friendlyTimeFormat.format(new Date(tm));
        else
            return " ";
    }


    public static void mkdir(final String dirpath) throws IOException
    {
        File dir = new File(dirpath);
        FileUtils.forceMkdir(dir);
    }


    public static Properties loadProperties(String propFile) throws IOException
    {
        Properties p = new Properties();
        URL url = ClassLoader.getSystemResource(propFile);
        if (url != null)
            p.load(url.openStream());
        return p;
    }


    public static String objectToString(Object o)
    {
        if (o.getClass().isArray())
            return ArrayUtils.toString(o);
        else
            return o.toString();
    }


    public static long checksumOptionalylUseFileLength(File file, int numBytes)
            throws IOException, FileNotFoundException, InterruptedException
    {
        long checksum = 0;
        try
        {
            checksum = checksum(file, numBytes);
        }
        catch (InsufficientFileLengthException e)
        {
            logger.info("file "
                    + file.getName()
                    + " is too short for a regular checksum. Use file length as an alternate.");

            checksum = checksum(file, (int) file.length());
        }

        return checksum;
    }


    public static long checksum(File file, int numBytes, int alternateNumBytes)
            throws IOException, FileNotFoundException,
            InsufficientFileLengthException, InterruptedException
    {
        long checksum = 0;
        try
        {
            checksum = checksum(file, numBytes);
        }
        catch (InsufficientFileLengthException e)
        {
            checksum = checksum(file, alternateNumBytes);
        }

        return checksum;
    }


    public static long checksum(File file, int numBytes) throws IOException,
            FileNotFoundException, InterruptedException,
            InsufficientFileLengthException
    {
        RandomAccessFile raf = null;
        try
        {
            raf = new RandomAccessFile(file, "r");
            long v = checksum(raf, numBytes);
            return v;
        }
        finally
        {
            IOUtils.closeQuietly(raf);
        }

    }


    public static long checksum(RandomAccessFile raFile, int numBytes)
            throws IOException, InsufficientFileLengthException
    {
        CRC32 _crc = new CRC32();

        long pos = raFile.getFilePointer();
        try
        {
            byte[] buffer = new byte[numBytes];
            raFile.seek(0);
            int n = raFile.read(buffer);
            if (n < numBytes)
            {
                String s;
                logger.warn(s = ("not enough data for checksum: current file size=" + n));
                throw new InsufficientFileLengthException(s);
            }

            synchronized (_crc)
            {
                _crc.reset();
                _crc.update(buffer);

                return _crc.getValue();
            }
        }
        finally
        {
            raFile.seek(pos);
        }

    }


    protected static RandomAccessFile openFile(File file)
            throws InterruptedException
    {
        RandomAccessFile reader = null;
        try
        {
            reader = new RandomAccessFile(file, "r");
            return reader;
        }
        catch (FileNotFoundException e)
        {
            return null;
        }
    }


    public static long checkSum(byte[] bytes)
    {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue();

    }


    public static boolean isPowerOfTwo(int number)
    {
        if (number <= 0)
        {
            throw new IllegalArgumentException("number: " + number);
        }

        return ((number & (number - 1)) == 0);
    }


    public static int power2(int size)
    {
        size--;
        size |= size >> 1;
        size |= size >> 2;
        size |= size >> 4;
        size |= size >> 8;
        size |= size >> 16;
        size++;
        return size;
    }


    public static String resolveThreadName(String id)
    {
        return ConfigConstants.QIAO + " " + id;
    }


    //TODO
    public static Path resolveNewFilePath(Path sourceFilePath, String newName)
    {
        return sourceFilePath.resolveSibling(newName);
    }


    public static String getQiaoConfigDir()
    {
        return System.getProperty(ConfigConstants.PROP_QIAO_CFG_DIR);
    }
}
