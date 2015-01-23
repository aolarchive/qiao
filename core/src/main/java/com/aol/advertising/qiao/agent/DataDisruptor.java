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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.aol.advertising.qiao.event.EventWrapper;
import com.aol.advertising.qiao.util.CommonUtils;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class DataDisruptor implements IDataPipe
{
    private int capacity = 1;
    private AtomicLong numWrites = new AtomicLong(0);
    private AtomicLong numReads = new AtomicLong(0);

    private RingBuffer<EventWrapper> ring;

    private Disruptor<EventWrapper> disruptor;
    private List<EventHandler<EventWrapper>> eventHandlerList = new ArrayList<EventHandler<EventWrapper>>();
    private WaitStrategy waitStrategy;
    private int threadPoolSize = 1;


    public void init() throws IOException
    {

        if (!CommonUtils.isPowerOfTwo(capacity))
            capacity = CommonUtils.power2(capacity);

        if (waitStrategy == null)
            waitStrategy = new BlockingWaitStrategy();

        //disruptor = new Disruptor<EventWrapper>(EventWrapper.EVENT_FACTORY,
        //        capacity, Executors.newCachedThreadPool(), ProducerType.SINGLE,
        //        waitStrategy);

        ThreadPoolTaskExecutor executor = CommonUtils.createFixedThreadPoolExecutor(threadPoolSize);
        executor.setThreadNamePrefix(this.getClass().getSimpleName());
        executor.initialize();

        disruptor = new Disruptor<EventWrapper>(EventWrapper.EVENT_FACTORY,
                capacity,
                executor,
                ProducerType.SINGLE, waitStrategy);

        disruptor.handleEventsWith(eventHandlerList
                .toArray(new EventHandler[0]));
    }


    @Override
    public void start()
    {
        ring = disruptor.start();
    }


    public void suspendEventHandlers()
    {
        disruptor.halt();
    }


    public void resumeEventHandlers()
    {
        disruptor.start();
    }


    public void shutdown()
    {
        disruptor.shutdown();
    }


    public void addEventHandler(EventHandler<EventWrapper> eventHandler)
    {
        eventHandlerList.add(eventHandler);
    }


    public void write(Object data)
    {
        long seq = ring.next();
        try
        {
            EventWrapper event = ring.get(seq);
            event.setEvent(data);
        }
        finally
        {
            ring.publish(seq);
        }

        numWrites.incrementAndGet();
    }


    public Object read()
    {

        numReads.incrementAndGet();

        return null; //TODO:
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
        return capacity - (int) ring.remainingCapacity();
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


    public long incrementAndGet()
    {
        return numReads.incrementAndGet();
    }


    public void setWaitStrategy(WaitStrategy waitStrategy)
    {
        this.waitStrategy = waitStrategy;
    }


    public int getThreadPoolSize()
    {
        return threadPoolSize;
    }


    public void setThreadPoolSize(int threadPoolSize)
    {
        this.threadPoolSize = threadPoolSize;
    }
}
