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
 * File Name:   DataPipe.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.agent;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class DataPipe implements IDataPipe
{
    private BlockingQueue<Object> queue;
    private int capacity = 1;
    private AtomicLong numWrites = new AtomicLong(0);
    private AtomicLong numReads = new AtomicLong(0);


    public void init() throws IOException
    {
        queue = new ArrayBlockingQueue<Object>(capacity, true);
    }


    @Override
    public void start() throws Exception
    {
    }


    public void write(Object data)
    {
        try
        {
            queue.put(data);
            numWrites.incrementAndGet();
        }
        catch (InterruptedException e)
        {
        }
    }


    public Object read() throws InterruptedException
    {
        Object o = queue.take();
        numReads.incrementAndGet();
        return o;
    }


    public int getCapacity()
    {
        return capacity;
    }


    public void setCapacity(int capacity)
    {
        this.capacity = capacity;
    }


    public int size()
    {
        return queue.size();
    }


    public boolean isEmpty()
    {
        return size() == 0;
    }


    public long getNumWrites()
    {
        return numWrites.get();
    }


    public long getNumReads()
    {
        return numReads.get();
    }


    public long getAndResetNumWrites()
    {
        return numWrites.getAndSet(0);
    }


    public long getAndResetNumReads()
    {
        return numReads.getAndSet(0);
    }

}
