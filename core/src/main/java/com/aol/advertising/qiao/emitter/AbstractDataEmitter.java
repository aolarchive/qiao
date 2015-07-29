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
