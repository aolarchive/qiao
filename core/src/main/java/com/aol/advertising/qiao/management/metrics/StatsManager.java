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
 * File Name:   StatsManager.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management.metrics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.util.CommonUtils;

@ManagedResource
public class StatsManager implements ApplicationListener<StatsEvent>
{
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected StatsCollector collector;
    protected StatsCalculator calculator;
    protected ScheduledExecutorService scheduler;
    protected Map<String, IStatisticsStore> statsStoreMap = new HashMap<String, IStatisticsStore>(); // key: funnel id

    private volatile boolean running = false;


    public void init() throws Exception
    {
        scheduler = CommonUtils.createScheduledExecutorService(1,
                CommonUtils.resolveThreadName("Stats"));
        if (collector != null)
        {
            collector.setScheduler(scheduler);
            collector.init();
        }

        if (calculator != null)
        {
            calculator.setScheduler(scheduler);
            calculator.init();
        }
    }


    public void start()
    {
        if (collector != null)
        {
            collector.start();
        }

        if (calculator != null)
        {
            calculator.start();
        }

        running = true;
    }


    @PreDestroy
    public void shutdown()
    {
        if (running)
        {
            running = false;

            if (collector != null)
            {
                collector.shutdown();
            }

            if (calculator != null)
            {
                calculator.shutdown();
            }

            scheduler.shutdown();
        }
    }


    @ManagedOperation(description = "Suspend stats calculator/collector")
    public void suspend()
    {
        if (collector != null)
        {
            collector.stop();
        }

        if (calculator != null)
        {
            calculator.stop();
        }
    }


    @ManagedOperation(description = "Resume stats calculator/collector")
    public void resume()
    {
        if (collector != null)
        {
            collector.restart();
        }

        if (calculator != null)
        {
            calculator.restart();
        }
    }


    @ManagedOperation(description = "Reset counters")
    public void resetCounters()
    {
        for (Iterator<Entry<String, IStatisticsStore>> iter = statsStoreMap
                .entrySet().iterator(); iter.hasNext();)
        {
            iter.next().getValue().resetCounters();
        }

        if (calculator != null)
        {
            calculator.resetCalcCounters();
        }
        
        logger.info("resetCounters done");
    }


    @ManagedAttribute
    public int getNumberOfStatsStores()
    {
        return statsStoreMap.size();
    }


    public StatsCollector getCollector()
    {
        return collector;
    }


    public void setCollector(StatsCollector collector)
    {
        this.collector = collector;
    }


    public StatsCalculator getCalculator()
    {
        return calculator;
    }


    public void setCalculator(StatsCalculator calculator)
    {
        this.calculator = calculator;
    }


    public IStatisticsStore getStatsStore(String id)
    {
        return statsStoreMap.get(id);
    }


    public void addStatsStore(String id, IStatisticsStore statsStore)
    {
        this.statsStoreMap.put(id, statsStore);
    }


    @ManagedAttribute
    public boolean isRunning()
    {
        return running;
    }


    @Override
    public void onApplicationEvent(StatsEvent event)
    {
        String funnel_id;
        if (!event.fromFunnel())
            funnel_id = event.getStatStoreId();

        else
            funnel_id = event.getFunnel().getId();

        IStatisticsStore stats_store = statsStoreMap.get(funnel_id);
        if (stats_store != null)
            stats_store.onStatsEvent(event);
    }

}
