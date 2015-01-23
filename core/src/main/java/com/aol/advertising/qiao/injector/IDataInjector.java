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
 * File Name:   IDataInjector.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector;

import org.springframework.context.ApplicationEventPublisherAware;

import com.aol.advertising.qiao.agent.IDataPipe;
import com.aol.advertising.qiao.management.IStatsCollectable;
import com.aol.advertising.qiao.management.ISuspendable;

/**
 * Interface for QIAO data injector.
 */
public interface IDataInjector extends ISuspendable,
        ApplicationEventPublisherAware, Runnable, IStatsCollectable
{
    public enum InjectorStatus
    {
        INACTIVE, ACTIVE, SUSPENDED
    };


    public void init() throws Exception;


    public void start() throws Exception;


    public void shutdown();


    boolean isRunning();


    public String getId();


    public void setId(String id);


    public void setFunnelId(String funnelId);


    public void setDataPipe(IDataPipe dataPipe);

}
