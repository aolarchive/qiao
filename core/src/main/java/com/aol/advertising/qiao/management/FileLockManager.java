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
 * File Name:   FileLockCache.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages virtual file locks.
 */
public class FileLockManager
{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private ConcurrentHashMap<Long, FileLockManager.FileLock> lockMap = new ConcurrentHashMap<Long, FileLockManager.FileLock>();


    public void addFileLock(long checksum)
    {
        if (logger.isDebugEnabled())
            logger.debug("added lock for checksum " + checksum);
        lockMap.put(checksum, new FileLock());
    }


    public void removeFileLock(long checksum)
    {
        if (logger.isDebugEnabled())
            logger.debug("remove lock for checksum " + checksum);
        lockMap.remove(checksum);
    }


    public boolean containsFileLock(long checksum)
    {
        return lockMap.containsKey(checksum);
    }


    // ----------------

    public static class FileLock
    {

    }
}
