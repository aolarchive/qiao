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
 * File Name:   BookKeepingCache.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.saf.core.util.cache.IPersistenceDataBinding;
import com.aol.advertising.saf.core.util.cache.PersistentCache;
import com.aol.advertising.saf.core.util.cache.PersistentValueWrapper;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;

/**
 * A cache to record files that have been processed. By default, an entry is
 * expired after 24 hours.
 */
public class BookKeepingCache
{
    private PersistentCache<Long, QiaoFileEntry> historyMap;
    private String cacheName;
    private String cacheDir;
    private int defaultExpirySecs = 86400; // default 1 day
    private int reapingIntervalSecs = 60;
    private int reapingInitDelaySecs = 60;
    private ScheduledThreadPoolExecutor scheduler;


    public void init() throws Exception
    {
        _validate();

        historyMap = new PersistentCache<Long, QiaoFileEntry>();
        historyMap.setId(this.getClass().getSimpleName());
        historyMap.setDbEnvDir(cacheDir);
        historyMap.setDatabaseName(cacheName);
        historyMap.setDataBinding(_createBinding());
        historyMap.setDefaultExpirySecs(defaultExpirySecs);
        historyMap.setDiskReapingIntervalSecs(reapingIntervalSecs);
        scheduler = CommonUtils.createScheduledThreadPoolExecutor(1,
                CommonUtils.resolveThreadName("History-Cache"));
        historyMap.setScheduler(scheduler);

        historyMap.start();
    }


    public void set(long key, QiaoFileEntry value)
    {
        historyMap.set(key, value);
    }


    public QiaoFileEntry get(long key)
    {
        return historyMap.get(key);
    }


    public QiaoFileEntry remove(long key)
    {
        return historyMap.delete(key);
    }


    public boolean contains(long key)
    {
        return historyMap.contain(key);
    }


    public void close()
    {
        historyMap.close();
        scheduler.shutdown();
    }


    private void _validate()
    {
        if (cacheName == null)
            throw new ConfigurationException("cacheName not defined");
        if (cacheDir == null)
            throw new ConfigurationException("cacheDir not defined");
    }


    private IPersistenceDataBinding<Long, PersistentValueWrapper<Long, QiaoFileEntry>> _createBinding()
    {
        return new IPersistenceDataBinding<Long, PersistentValueWrapper<Long, QiaoFileEntry>>()
        {

            @Override
            public EntryBinding<Long> getKeyBinding(
                    StoredClassCatalog clzCatalog)
            {
                return TupleBinding.getPrimitiveBinding(Long.class);
            }


            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override
            public EntryBinding<PersistentValueWrapper<Long, QiaoFileEntry>> getValueBinding(
                    StoredClassCatalog clzCatalog)
            {
                return new SerialBinding(clzCatalog,
                        PersistentValueWrapper.class);
            }

        };
    }


    public void setCacheName(String cacheName)
    {
        this.cacheName = cacheName;
    }


    public void setCacheDir(String cacheDir)
    {
        this.cacheDir = cacheDir;
    }


    public void setDefaultExpirySecs(int defaultExpirySecs)
    {
        this.defaultExpirySecs = defaultExpirySecs;
    }


    public void setReapingIntervalSecs(int reapingIntervalSecs)
    {
        this.reapingIntervalSecs = reapingIntervalSecs;
    }


    public String dumpHistory()
    {
        return historyMap.dump();
    }


    public int getReapingInitDelaySecs()
    {
        return reapingInitDelaySecs;
    }


    public void setReapingInitDelaySecs(int reapingInitDelaySecs)
    {
        this.reapingInitDelaySecs = reapingInitDelaySecs;
    }


    public int getDefaultExpirySecs()
    {
        return defaultExpirySecs;
    }


    public int getReapingIntervalSecs()
    {
        return reapingIntervalSecs;
    }
}
