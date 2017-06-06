/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2013 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   QiaoFileBookKeeper.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.injector.file.IFileOperationListener;
import com.aol.advertising.qiao.util.CommonUtils;

/**
 * Keeps a record of files that have been processed. The qiaoLogStorePath
 * property defines the file location of the record. In addition, onComplete and
 * onPartialComplete are called. the file's meta data is saved on disk, indexed
 * by its checksum value.
 */
public class QiaoFileBookKeeper implements IFileOperationListener
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String qiaoLogStorePath;
    private BufferedWriter writer;
    private AtomicBoolean isInitialized = new AtomicBoolean(false);
    private BookKeepingCache historyCache;
    private String historyCacheName = "historyCache";
    private String historyCacheDir;
    private int cacheDefaultExpirySecs = 86400;
    private int cacheDiskReapingIntervalSecs = 60;
    private int cacheDiskReapingInitDelaySecs = 60;


    public void init()
    {
        if (isInitialized.compareAndSet(false, true))
        {
            _validate();

            try
            {
                historyCache = new BookKeepingCache();
                historyCache.setCacheDir(historyCacheDir);
                historyCache.setCacheName(historyCacheName);
                historyCache.setDefaultExpirySecs(cacheDefaultExpirySecs);
                historyCache
                        .setReapingInitDelaySecs(cacheDiskReapingInitDelaySecs);
                historyCache
                        .setReapingIntervalSecs(cacheDiskReapingIntervalSecs);
                historyCache.init();

                writer = openFileForWrite(qiaoLogStorePath);
            }
            catch (IOException e)
            {
                throw new ConfigurationException("Unable to open "
                        + qiaoLogStorePath + " => " + e.getMessage());
            }
            catch (Exception e)
            {
                throw new ConfigurationException("Failed to initialize: "
                        + e.getMessage(), e);
            }

            logger.info(this.getClass().getName() + " initialized");
        }

    }


    private void _validate()
    {
        if (qiaoLogStorePath == null)
            throw new ConfigurationException("qiaoLogStorePath not defined");

        if (historyCacheDir == null)
            throw new ConfigurationException("historyCacheDir not defined");

        if (historyCacheName == null)
            throw new ConfigurationException("historyCacheName not defined");
    }


    private BufferedWriter openFileForWrite(String logPathname)
            throws IOException
    {
        Path log_path = FileSystems.getDefault().getPath(logPathname);

        return Files.newBufferedWriter(log_path, CommonUtils.UTF8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }


    public void close()
    {

        flushWriter();
        IOUtils.closeQuietly(writer);

        historyCache.close();

        isInitialized.set(false);

        logger.info(this.getClass().getName() + " closed");

    }


    private void flushWriter()
    {
        try
        {
            writer.flush();
        }
        catch (IOException e)
        {
            logger.warn(e.getMessage());
        }

    }


    public void write(QiaoFileEntry logEntry) throws IOException
    {
        writer.write(CommonUtils.getFriendlyTimeString(System
                .currentTimeMillis()) + "," + logEntry.toCsv());
        writer.newLine();
        flushWriter();

    }


    public QiaoFileEntry getHistory(long checksum)
    {
        return historyCache.get(checksum);
    }


    public QiaoFileEntry deleteHistory(long checksum)
    {
        return historyCache.remove(checksum);
    }


    public String getQiaoLogStorePath()
    {
        return qiaoLogStorePath;
    }


    public void setQiaoLogStorePath(String qiaoLogStorePath)
    {
        this.qiaoLogStorePath = qiaoLogStorePath;
    }


    @Override
    public void onOpen(String filename)
    {
        if (logger.isDebugEnabled())
            logger.debug("onOpen> " + filename);
    }


    @Override
    public void onComplete(QiaoFileEntry file)
    {
        logger.info("onComplete> " + file.toString());
        try
        {
            write(file);

            historyCache.set(file.checksum, file);

        }
        catch (IOException e)
        {
            logger.error("Unable to write " + file);
        }
    }


    public String dumpHistoryCache()
    {
        return historyCache.dumpHistory();
    }


    public void setHistoryCacheName(String historyCacheName)
    {
        this.historyCacheName = historyCacheName;
    }


    public void setHistoryCacheDir(String historyCacheDir)
    {
        this.historyCacheDir = historyCacheDir;
    }


    public int getCacheDefaultExpirySecs()
    {
        return cacheDefaultExpirySecs;
    }


    public void setCacheDefaultExpirySecs(int cacheDefaultExpirySecs)
    {
        this.cacheDefaultExpirySecs = cacheDefaultExpirySecs;
    }


    public int getCacheDiskReapingIntervalSecs()
    {
        return cacheDiskReapingIntervalSecs;
    }


    public void setCacheDiskReapingIntervalSecs(int cacheDiskReapingIntervalSecs)
    {
        this.cacheDiskReapingIntervalSecs = cacheDiskReapingIntervalSecs;
    }


    public int getCacheDiskReapingInitDelaySecs()
    {
        return cacheDiskReapingInitDelaySecs;
    }


    public void setCacheDiskReapingInitDelaySecs(
            int cacheDiskReapingInitDelaySecs)
    {
        this.cacheDiskReapingInitDelaySecs = cacheDiskReapingInitDelaySecs;
    }

}
