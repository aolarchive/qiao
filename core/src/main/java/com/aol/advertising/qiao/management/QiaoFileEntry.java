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
 * File Name:   QiaoFileEntry.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class QiaoFileEntry implements Externalizable
{
    String filename;
    long checksum;
    long timeProcessed;
    long offset;
    long lastFileModTime;
    boolean completed;


    public QiaoFileEntry()
    {
        // Use for de-serialization only
    }


    public QiaoFileEntry(String filename, long lastFileModTime, long checksum,
            long timeProcessed, long offset, boolean completed)
    {
        this.filename = filename;
        this.lastFileModTime = lastFileModTime;
        this.checksum = checksum;
        this.timeProcessed = timeProcessed;
        this.offset = offset;
        this.completed = completed;
    }


    public String toCsv()
    {
        return filename + "," + checksum + "," + offset + "," + lastFileModTime
                + "," + timeProcessed + "," + completed;
    }


    public String toString()
    {
        return toCsv();
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeLong(checksum);
        out.writeLong(timeProcessed);
        out.writeLong(offset);
        out.writeLong(lastFileModTime);
        out.writeBoolean(completed);
        out.writeUTF(filename);
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException
    {
        checksum = in.readLong();
        timeProcessed = in.readLong();
        offset = in.readLong();
        lastFileModTime = in.readLong();
        completed = in.readBoolean();
        filename = in.readUTF();

    }


    public String getFilename()
    {
        return filename;
    }


    public long getChecksum()
    {
        return checksum;
    }


    public long getTimeProcessed()
    {
        return timeProcessed;
    }


    public long getOffset()
    {
        return offset;
    }


    public long getLastFileModTime()
    {
        return lastFileModTime;
    }


    public boolean isCompleted()
    {
        return completed;
    }
}
