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
 * File Name:   DummyEmitter.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.management.metrics.StatsCollector;

/**
 * Internal use only.
 */
@ManagedResource
public class DummyEmitter extends AbstractDataEmitter
{
    private AtomicLong numOutput = new AtomicLong(0);


    @Override
    public void process(Object data)
    {
        numOutput.incrementAndGet();
    }


    @Override
    public void removeThreadLocal()
    {
    }


    @Override
    public void setStatsCollector(StatsCollector statsCollector)
    {
    }


    @Override
    public void setEmitterThreadCount(int threadCount)
    {
    }

}
