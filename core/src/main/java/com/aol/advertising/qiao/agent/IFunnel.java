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
 * File Name:   IFunnel.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.agent;

import com.aol.advertising.qiao.emitter.IDataEmitterContainer;
import com.aol.advertising.qiao.injector.IDataInjector;
import com.aol.advertising.qiao.management.IStatsCalculatorAware;
import com.aol.advertising.qiao.management.IStatsCollectable;
import com.aol.advertising.qiao.management.metrics.IStatisticsStore;

public interface IFunnel extends IStatsCollectable, IStatsCalculatorAware
{
    public void setDataInjector(IDataInjector src);


    public void setDataEmitter(IDataEmitterContainer sink);


    public void init() throws Exception;


    public void start() throws Exception;


    public void close();


    public String getId();


    public void setId(String id);;


    public void setEmitterThreadCount(int sinkThreadCount);


    public void setDataPipeCapacity(int dataPipeCapacity);


    public boolean isRunning();


    public void setStatsStore(IStatisticsStore statsStore);


    public void suspend();


    public void drainThenSuspend();


    public void resume();


    public void setAutoStart(boolean autoStart);


    public void setRateLimit(int targetLimitedRate);

}
