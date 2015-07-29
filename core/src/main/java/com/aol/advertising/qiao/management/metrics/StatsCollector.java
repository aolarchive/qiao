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

package com.aol.advertising.qiao.management.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.management.jmx.StatsControl;

/**
 * The StatsCollector object collects statistics metrics periodically.
 */
@ManagedResource(description = "Metrics Collector")
public class StatsCollector implements Runnable,
        ApplicationListener<StatsControl>
{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private List<Callable< ? >> statsCallable = Collections
            .synchronizedList(new ArrayList<Callable< ? >>());
    private long intervalSecs = 60;
    private long initDelaySecs = 15;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture< ? > schedulerHandler;
    private boolean running = false;


    /**
     * Initialize.
     */
    public void init()
    {
    }


    /**
     * Start the collection process.
     */
    public void start()
    {
        schedulerHandler = scheduler.scheduleAtFixedRate(this, initDelaySecs,
                intervalSecs, TimeUnit.SECONDS);
        logger.info("stats collector starts");
        running = true;
    }


    /**
     * Terminate the collection process.
     */
    @ManagedOperation(description = "Stop the collection process")
    public void stop()
    {
        if (running)
        {
            schedulerHandler.cancel(false);
            logger.info("stats collector stops");
            running = false;
        }
    }


    @ManagedOperation(description = "Restart the collection process")
    public void restart()
    {
        if (!running)
        {
            schedulerHandler = scheduler.scheduleAtFixedRate(this,
                    initDelaySecs, intervalSecs, TimeUnit.SECONDS);

            logger.info("stats collector restarts");
            running = true;
        }
        else
        {
            logger.info("stats collector already running");
        }
    }


    public void shutdown()
    {
        stop();
        scheduler.shutdown();
    }


    @Override
    public void run()
    {
        for (Callable< ? > c : statsCallable)
        {
            try
            {
                c.call();
            }
            catch (Throwable e)
            {
                logger.error(e.getMessage(), e);
            }
        }

    }


    /**
     * Register the callable object to be invoked at each interval.
     *
     * @param c
     */
    public void register(Callable< ? > c)
    {
        statsCallable.add(c);
    }


    /**
     * Unregister the previously registered callable object.
     *
     * @param c
     */
    public void unregister(Callable< ? > c)
    {
        statsCallable.remove(c);
    }


    /**
     * Set time in seconds between successive collector runs.
     *
     * @param intervalSecs
     */
    public void setIntervalSecs(long intervalSecs)
    {
        this.intervalSecs = intervalSecs;
    }


    @ManagedAttribute
    public boolean isRunning()
    {
        return running;
    }


    @Override
    public void onApplicationEvent(StatsControl event)
    {
        switch (event.getCommand())
        {
            case START_STATS:
                restart();
                break;
            case STOP_STATS:
                stop();
                break;
            default:
                ;
        }

    }


    public void setInitDelaySecs(long initDelaySecs)
    {
        this.initDelaySecs = initDelaySecs;
    }


    public void setScheduler(ScheduledExecutorService scheduler)
    {
        this.scheduler = scheduler;
    }

}
