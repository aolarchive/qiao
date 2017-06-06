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

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.aol.advertising.qiao.agent.IDataPipe;
import com.aol.advertising.qiao.management.metrics.StatsCollector;
import com.aol.advertising.qiao.management.metrics.StatsEvent;
import com.aol.advertising.qiao.management.metrics.StatsEvent.StatsOp;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.RateLimiter;

@ManagedResource
public class RandomIntegerDataSource implements IDataInjector
{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private int maxValue = 100;
    private ThreadLocal<Random> randLocal = new ThreadLocal<Random>();
    private IDataPipe dataPipe;
    protected String funnelId;
    private String id = this.getClass().getSimpleName();
    private String agentId;

    private volatile boolean running = false;
    private ThreadPoolTaskExecutor executor;
    private int threadCount = 1;

    private ApplicationEventPublisher eventPublisher;
    private AtomicLong numGenerated = new AtomicLong(0);

    private StatsCollector statsCollector;
    private RateLimiter rateLimit;
    private int targetRate = 0;

    private AtomicBoolean isSuspended = new AtomicBoolean(false);


    @Override
    public void init() throws Exception
    {
        _registerStatsCollector();

        executor = CommonUtils.createFixedThreadPoolExecutor(threadCount);
        executor.setThreadNamePrefix(id);
        executor.initialize();

        if (targetRate > 0)
            rateLimit = RateLimiter.create(targetRate);

        logger.info(this.getClass().getName() + " initialized");

    }


    protected void _registerStatsCollector()
    {
        if (statsCollector != null)
            statsCollector.register(new Callable<Void>()
            {
                @Override
                public Void call()
                {
                    if (numGenerated.get() > 0)
                        eventPublisher.publishEvent(new StatsEvent(this, this
                                .getClass().getSimpleName(), funnelId,
                                StatsOp.INCRBY, "random_input", numGenerated
                                        .getAndSet(0)));

                    return null;
                }

            });
    }


    @Override
    public void start() throws Exception
    {
        for (int i = 0; i < threadCount; i++)
        {
            executor.submit(this);
        }
        running = true;
        logger.info(this.getClass().getSimpleName() + " started");
    }


    @Override
    public void run()
    {
        while (running)
        {
            if (rateLimit != null)
                try
                {
                    rateLimit.acquire();
                }
                catch (InterruptedException e)
                {
                    break;
                }

            Random rand = randLocal.get();
            if (rand == null)
            {
                rand = new Random(System.currentTimeMillis());
                randLocal.set(rand);
            }

            int v = rand.nextInt(maxValue);
            logger.info("get> " + v);

            numGenerated.incrementAndGet();
            dataPipe.write(v);
        }

        logger.info(this.getClass().getSimpleName() + " terminated");

    }


    @Override
    public void shutdown()
    {
        running = false;
        if (executor != null)
            executor.destroy();
        if (rateLimit != null)
            rateLimit.destroy();
    }


    @ManagedAttribute
    public int getMaxValue()
    {
        return maxValue;
    }


    public void setMaxValue(int maxValue)
    {
        this.maxValue = maxValue;
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
        this.dataPipe = dataPipe;
    }


    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher eventPublisher)
    {
        this.eventPublisher = eventPublisher;
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
        this.statsCollector = statsCollector;

    }


    @Override
    public void setFunnelId(String funnelId)
    {
        this.funnelId = funnelId;
    }


    @ManagedAttribute
    public long getNumGenerated()
    {
        return numGenerated.get();
    }


    public int getTargetRate()
    {
        return targetRate;
    }


    public void setTargetRate(int targetRate)
    {
        this.targetRate = targetRate;
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


    @Override
    public void setAgentId(String agentId)
    {
        this.agentId = agentId;
    }
}
