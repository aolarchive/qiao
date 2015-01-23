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
 * File Name:   IDataEmitter.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter;

import org.springframework.context.ApplicationEventPublisherAware;

import com.aol.advertising.qiao.management.IStatsCollectable;
import com.aol.advertising.qiao.management.ISuspendable;

/**
 * An interface for a data emitter.
 */
public interface IDataEmitter extends ApplicationEventPublisherAware,
        IStatsCollectable, ISuspendable
{
    public void init() throws Exception;


    public void start() throws Exception;


    public void shutdown();


    /**
     * Remove thread local resources. Called when agent is suspended.
     */
    public void removeThreadLocal();


    public String getId();


    public void setFunnelId(String funnelId);


    public void setId(String id);


    public void process(Object data);


    public boolean isRunning();


    public void setEmitterThreadCount(int threadCount);

}
