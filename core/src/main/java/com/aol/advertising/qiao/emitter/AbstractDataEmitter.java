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
 * File Name:   AbstractDataEmitter.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.management.metrics.StatsCollector;

@ManagedResource
public abstract class AbstractDataEmitter implements IDataEmitter
{
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected String funnelId;
    protected String id;
    protected volatile boolean running = false; // this indicates ready-ness
    protected AtomicBoolean isSuspended = new AtomicBoolean(false);

    protected ApplicationEventPublisher eventPublisher;
    protected StatsCollector statsCollector;


    @Override
    public void init() throws Exception
    {
    }


    @Override
    public void start() throws Exception
    {
        running = true;
    }


    @Override
    public void shutdown()
    {
        running = false;
    }


    @ManagedAttribute
    @Override
    public String getId()
    {
        return id;
    }


    @Override
    public void setFunnelId(String funnelId)
    {
        this.funnelId = funnelId;
    }


    public void setId(String id)
    {
        this.id = id;
    }


    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher publisher)
    {
        this.eventPublisher = publisher;
    }


    @Override
    public void setStatsCollector(StatsCollector statsCollector)
    {
        this.statsCollector = statsCollector;
    }


    public String getFunnelId()
    {
        return funnelId;
    }


    @ManagedAttribute
    public boolean isRunning()
    {
        return running;
    }


    @Override
    public void suspend()
    {
        isSuspended.compareAndSet(false, true);
    }


    @Override
    public void resume()
    {
        isSuspended.compareAndSet(true, false);
    }


    @Override
    public boolean isSuspended()
    {
        return isSuspended.get();
    }

}
