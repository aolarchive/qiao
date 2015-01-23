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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.cache.PositionCache;
import com.sleepycat.je.Transaction;

/**
 * A convenient class to interact with PositionCache. Note that
 * FileReadingPositionCache can only process one file at a time. Do not
 * interleave position recording with multiple files simultaneously.
 * <p>
 * Usage: Invoke setKey(checksum) first when a new file is encountered. Later
 * interactions are assumed to work with the given checksum.
 */
public class FileReadingPositionCache
{
    public static class FileReadState implements Externalizable
    {
        public long position = 0; // next read offset
        public long timestamp = 0; // last read time
        public long checksum = 0;


        @Override
        public void writeExternal(ObjectOutput out) throws IOException
        {
            out.writeLong(position);
            out.writeLong(timestamp);
            out.writeLong(checksum);
        }


        @Override
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException
        {
            position = in.readLong();
            timestamp = in.readLong();
            checksum = in.readLong();
        }


        public String toString()
        {
            return "pos=" + position + ", checksum=" + checksum
                    + ", read_time=" + timestamp;
        }


        public String getFriendlyTimestamp()
        {
            return CommonUtils.getFriendlyTimeString(timestamp);
        }


        public FileReadState copy()
        {
            FileReadState clone = new FileReadState();
            clone.position = this.position;
            clone.timestamp = this.timestamp;
            clone.checksum = this.checksum;

            return clone;
        }
    }

    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private final PositionCache readPositionMap;

    private long _key; // checksum of a file
    private FileReadState _value;


    /**
     * Constructor. The key and the value are populated at this time.
     * 
     * @param key
     * @param readPositionMap
     * @throws Exception
     */
    public FileReadingPositionCache(PositionCache positionMap) throws Exception
    {
        readPositionMap = positionMap;

        this._value = new FileReadingPositionCache.FileReadState();
    }


    public void setKey(long key)
    {
        this._key = key;

        long v = get();
        if (v == -1L)
            set(0L, 0L, key);
    }


    public long get()
    {
        FileReadingPositionCache.FileReadState v = readPositionMap.get(_key);
        if (v == null)
            return -1;

        _value.timestamp = v.timestamp;
        _value.position = v.position;
        _value.checksum = v.checksum;
        return v.position;
    }


    /**
     * Returns the previous position, or null if there was none.
     * 
     * @param offset
     * @return
     */
    public long set(long offset)
    {
        return this.set(offset, System.currentTimeMillis());
    }


    /**
     * Returns the previous position, or -1 if there was none.
     * 
     * 
     * @param offset
     * @param timestamp
     * @return
     */
    public long set(long offset, long timestamp)
    {
        _value.position = offset;
        _value.timestamp = timestamp;
        FileReadingPositionCache.FileReadState old = readPositionMap.set(_key,
                _value);
        return (old == null ? -1 : old.position);
    }


    public long set(long offset, long timestamp, long checksum)
    {
        _value.position = offset;
        _value.timestamp = timestamp;
        _value.checksum = checksum;
        FileReadingPositionCache.FileReadState old = readPositionMap.set(_key,
                _value);
        return (old == null ? -1 : old.position);
    }


    public long set(long offset, boolean autoCommit)
    {
        return this.set(offset, System.currentTimeMillis(), autoCommit);
    }


    public long set(long offset, long timestamp, boolean autoCommit)
    {
        if (autoCommit)
            beginTransaction();

        long v = this.set(offset, timestamp);

        if (autoCommit)
            commit();

        return v;

    }


    public void save()
    {
        readPositionMap.set(_key, _value);
    }


    public FileReadState remove()
    {
        return remove(_key);
    }


    public Transaction beginTransaction()
    {
        try
        {
            return readPositionMap.beginTransaction();
        }
        catch (IllegalStateException e)
        {
            logger.error("beginTransaction> " + e.getMessage(), e);
        }
        return null;
    }


    public void abortTransaction()
    {
        readPositionMap.abortTransaction();
    }


    public void commit()
    {
        readPositionMap.commitTransaction();
    }


    public long getLastTimestamp()
    {
        return _value.timestamp;
    }


    public Long getKey()
    {
        return _key;
    }


    public FileReadState getReadState()
    {
        return _value;
    }


    public String toString()
    {
        return _value.toString();
    }


    // -----------------------

    public void set(long key, FileReadingPositionCache.FileReadState value)
    {
        readPositionMap.set(key, value);
    }


    public FileReadState getState(long key)
    {
        return readPositionMap.get(key);
    }


    public FileReadState remove(long key)
    {
        return readPositionMap.delete(key);
    }


    public boolean contains(long key)
    {
        return readPositionMap.contain(key);
    }


    /**
     * Close the position map.
     */
    public void close()
    {
        try
        {
            readPositionMap.close();
        }
        catch (Throwable t)
        {
            logger.warn("close exception: " + t.getClass().getSimpleName()
                    + ": " + t.getMessage());
        }
    }


    public String dumpCache()
    {
        return readPositionMap.dump();
    }


    public String dumpHistory()
    {
        return readPositionMap.dump();
    }
}
