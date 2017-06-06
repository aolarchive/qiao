/****************************************************************************
 * Copyright (c) 2015 AOL Inc.
 * @author:     ytung05
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ****************************************************************************/

package com.aol.advertising.qiao.injector;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.agent.IDataPipe;
import com.aol.advertising.qiao.management.metrics.StatsCollector;
import com.aol.advertising.qiao.util.CommonUtils;

@ManagedResource
public class DummyDataSource implements IDataInjector
{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String id = this.getClass().getSimpleName();
    private String funnelId;
    private String agentId;

    private volatile boolean running = false;

    private AtomicBoolean isSuspended = new AtomicBoolean(false);


    @Override
    public void init() throws Exception
    {
        logger.info("<init>");
    }


    @Override
    public void start() throws Exception
    {
        running = true;
        logger.info(this.getClass().getSimpleName() + " started");
    }


    @Override
    public void run()
    {
        while (running)
        {
            CommonUtils.sleepQuietly(1000);
        }

        logger.info(this.getClass().getSimpleName() + " terminated");

    }


    @Override
    public void shutdown()
    {
        running = false;
    }


    @ManagedAttribute
    @Override
    public boolean isRunning()
    {

        return running;
    }


    @Override
    public void setDataPipe(IDataPipe dataPipe)
    {

    }


    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher eventPublisher)
    {
    }


    public void setId(String id)
    {
        this.id = id;
    }


    @ManagedAttribute
    @Override
    public String getId()
    {
        return id;
    }


    @Override
    public void setStatsCollector(StatsCollector statsCollector)
    {
    }


    @Override
    public void setFunnelId(String funnelId)
    {
        this.funnelId = funnelId;
    }


    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            shutdown();
        }
    }


    @Override
    public void resume()
    {
        if (isSuspended.compareAndSet(true, false))
        {
            try
            {
                start();
            }
            catch (Exception e)
            {
                logger.error(
                        "failed to resume the opration => " + e.getMessage(), e);
            }
        }
    }


    @Override
    public boolean isSuspended()
    {
        return isSuspended.get();
    }


    public String getFunnelId()
    {
        return funnelId;
    }


    @Override
    public void setAgentId(String agentId)
    {
        this.agentId = agentId;

    }
}
