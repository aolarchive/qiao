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
 * File Name:   DataFunnel.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.agent;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.config.ConfigConstants;
import com.aol.advertising.qiao.emitter.IDataEmitterContainer;
import com.aol.advertising.qiao.injector.IDataInjector;
import com.aol.advertising.qiao.management.metrics.IStatisticsStore;
import com.aol.advertising.qiao.management.metrics.PubStats;
import com.aol.advertising.qiao.management.metrics.StatsCalculator;
import com.aol.advertising.qiao.management.metrics.StatsCollector;
import com.aol.advertising.qiao.management.metrics.StatsEnum;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.ContextUtils;

/**
 * DataFunnel transports data from an injector to emitters.
 */
@ManagedResource
public class DataFunnel implements IFunnel, Runnable
{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String id;
    private IDataInjector injector;
    private IDataEmitterContainer emitterContainer;
    private volatile boolean running = false;
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private AtomicBoolean isSuspended = new AtomicBoolean(false);
    private boolean autoStart = true;
    protected int targetLimitedRate = 0;

    private String dataPipeClassname;
    private IDataPipe dataPipe;
    private int dataPipeCapacity;
    private int emitterThreadCount = 1;
    private StatsCollector statsCollector;
    private StatsCalculator statsCalculator;
    private IStatisticsStore statsStore;

    // 
    private Map<String, PubStats> counterKeys = new LinkedHashMap<String, PubStats>();

    private ScheduledExecutorService scheduler;
    private int initDelaySecs = 10;
    private int intervalSecs = 5;
    private ScheduledFuture< ? > schedFuture;


    @Override
    public void init() throws Exception
    {

        if (statsStore != null)
        {
            statsStore.setId(id);
            statsStore.init();
        }

        dataPipe = createDataPipe();
        injector.setDataPipe(dataPipe);
        emitterContainer.setDataPipe(dataPipe);
        emitterContainer.setEmitterThreadCount(emitterThreadCount);
        emitterContainer.setFunnelId(id); // pass funnel id 
        emitterContainer.setRateLimit(targetLimitedRate);
        emitterContainer.init();
        injector.init();

        dataPipe.init();

        _registerStatsCollector();
        _registerStatsCalculator();

        scheduler = CommonUtils.createScheduledExecutorService(1,
                CommonUtils.resolveThreadName("DataFunnel_" + id));

        logger.info(this.getClass().getSimpleName() + " initialized");

    }


    private IDataPipe createDataPipe() throws Exception
    {
        if (dataPipeClassname == null)
            dataPipeClassname = ConfigConstants.DEFAULT_FUNNEL_DATAPIPE_CLASSNAME;

        IDataPipe pipe = (IDataPipe) ContextUtils
                .getOrCreateBean(dataPipeClassname);
        pipe.setCapacity(dataPipeCapacity);

        return pipe;

    }


    private void _registerStatsCollector()
    {
        if (statsCollector != null)
        {
            statsCollector.register(new Callable<Void>()
            {
                @Override
                public Void call()
                {
                    long inputs = dataPipe.getAndResetNumWrites();
                    if (inputs > 0)
                    {
                        statsStore.incr(StatsEnum.EVENT_INPUT_COUNT.value(),
                                inputs);
                    }

                    long outputs = dataPipe.getAndResetNumReads();
                    if (outputs > 0)
                    {
                        statsStore.incr(StatsEnum.PROCESSED_COUNT.value(),
                                outputs);
                    }

                    return null;
                }

            });
        }

    }


    private void _registerStatsCalculator()
    {
        if (statsCalculator != null)
        {
            initCounters();
            statsCalculator.register(statsCalculator.new CalcCallable(
                    statsStore, counterKeys));
        }

    }


    private void initCounters()
    {
        if (counterKeys.size() == 0)
        {
            PubStats pstats = new PubStats(StatsEnum.EVENT_INPUT_COUNT.value(),
                    false, true, true, false);
            counterKeys.put(pstats.getMetric(), pstats);
            pstats = new PubStats(StatsEnum.EVENT_OUTPUT_COUNT.value(), false,
                    true, true, false);
            counterKeys.put(pstats.getMetric(), pstats);
            pstats = new PubStats(StatsEnum.PROCESSED_COUNT.value(), false,
                    true, true, false);
            counterKeys.put(pstats.getMetric(), pstats);

            pstats = new PubStats(StatsEnum.DATAPIPE_QUEUELEN.value(), false,
                    false, false, true);
            pstats.setGauge(new Callable<Integer>()
            {

                @Override
                public Integer call() throws Exception
                {
                    return dataPipe.size();
                }
            });
            counterKeys.put(pstats.getMetric(), pstats);
        }

    }


    @Override
    public void start() throws Exception
    {
        if (autoStart && isStarted.compareAndSet(false, true))
        {
            _start();
        }

    }


    private void _start() throws Exception
    {
        dataPipe.start();

        emitterContainer.start();
        injector.start();

        running = true;

        schedFuture = scheduler.scheduleAtFixedRate(this, this.initDelaySecs,
                this.intervalSecs, TimeUnit.SECONDS);

        logger.info(this.getClass().getSimpleName() + " started");

    }


    @ManagedOperation
    public void manualStart() throws Exception
    {
        if (!autoStart && isStarted.compareAndSet(false, true))
        {
            _start();
        }

    }


    @Override
    public void close()
    {
        running = false;

        if (schedFuture != null)
            schedFuture.cancel(false);
        if (scheduler != null)
            scheduler.shutdown();
        if (injector != null)
            injector.shutdown();
        if (emitterContainer != null)
            emitterContainer.shutdown();

        logger.info(this.getClass().getSimpleName() + " closed");
    }


    @Override
    public void setDataInjector(IDataInjector src)
    {
        this.injector = src;
    }


    @Override
    public void setDataEmitter(IDataEmitterContainer sink)
    {
        this.emitterContainer = sink;
    }


    @ManagedAttribute
    public int getEmitterThreadCount()
    {
        return emitterThreadCount;
    }


    public void setId(String id)
    {
        this.id = id;
    }


    public void setDataPipe(DataPipe dataPipe)
    {
        this.dataPipe = dataPipe;
    }


    public void setEmitterThreadCount(int sinkThreadCount)
    {
        this.emitterThreadCount = sinkThreadCount;
    }


    @ManagedAttribute
    @Override
    public boolean isRunning()
    {
        return running && isStarted.get();
    }


    @ManagedAttribute
    public String getId()
    {
        return id;
    }


    public void setStatsCollector(StatsCollector statsCollector)
    {
        this.statsCollector = statsCollector;
    }


    @ManagedAttribute
    public int getQueueSize()
    {
        return dataPipe.size();
    }


    @ManagedAttribute
    public int getQueueCapacity()
    {
        return dataPipe.getCapacity();
    }


    public void setStatsStore(IStatisticsStore statsStore)
    {
        this.statsStore = statsStore;
    }


    public void setStatsCalculator(StatsCalculator statsCalculator)
    {
        this.statsCalculator = statsCalculator;
    }


    @ManagedOperation
    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            running = false;

            injector.suspend();
            emitterContainer.suspend();

            scheduler.shutdown();

            logger.info("Funnel " + id + " suspended");
        }
        else
            logger.warn("Funnel " + id + " was already suspended");
    }


    @ManagedOperation
    public void drainThenSuspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            running = false;
            injector.suspend();
            CommonUtils.sleepQuietly(10);
            emitterContainer.drainThenSuspend();

            scheduler.shutdown();

            logger.info("Funnel " + id + " suspended");
        }
        else
            logger.warn("Funnel " + id + " was already suspended");
    }


    @ManagedOperation
    @Override
    public void resume()
    {
        if (isSuspended.compareAndSet(true, false))
        {
            injector.resume();
            emitterContainer.resume();

            scheduler = CommonUtils.createScheduledExecutorService(1,
                    CommonUtils.resolveThreadName("DataFunnel_" + id));
            schedFuture = scheduler.scheduleAtFixedRate(this,
                    this.initDelaySecs, this.intervalSecs, TimeUnit.SECONDS);

            running = true;

            logger.info("Funnel " + id + " operation resumed");
        }
        else
            logger.warn("Nothing to do - Funnel " + id + " was not suspended");

    }


    public void setDataPipeClassname(String dataPipeClassname)
    {
        this.dataPipeClassname = dataPipeClassname;
    }


    @Override
    public void setDataPipeCapacity(int dataPipeCapacity)
    {
        this.dataPipeCapacity = dataPipeCapacity;
    }


    @Override
    public void setAutoStart(boolean autoStart)
    {
        this.autoStart = autoStart;
    }


    @ManagedAttribute
    public boolean isAutoStart()
    {
        return autoStart;
    }


    @ManagedAttribute
    public boolean getIsSuspended()
    {
        return isSuspended.get();
    }


    @ManagedAttribute
    public int getTargetLimitedRate()
    {
        return targetLimitedRate;
    }


    @Override
    public void setRateLimit(int targetLimitedRate)
    {
        this.targetLimitedRate = targetLimitedRate;
    }


    @ManagedOperation
    public void changeRateLimit(int targetLimitedRate)
    {
        this.targetLimitedRate = targetLimitedRate;
        this.emitterContainer.changeRateLimit(targetLimitedRate);
    }


    @Override
    public void run()
    {
        if (!isSuspended.get())
        {
            if ((!injector.isRunning()) && (!injector.isSuspended()))
            {
                logger.warn(injector.getId()
                        + " not in running state.  Drain and suspend the funnel");
                this.drainThenSuspend();
            }
            else if (!emitterContainer.isRunning()
                    && !emitterContainer.isSuspended())
            {
                logger.warn(emitterContainer.getId()
                        + " not in running state.  Suspend the funnel");
                this.suspend();
            }
        }
    }


    public void setInitDelaySecs(int initDelaySecs)
    {
        this.initDelaySecs = initDelaySecs;
    }


    public void setIntervalSecs(int intervalSecs)
    {
        this.intervalSecs = intervalSecs;
    }


    @ManagedAttribute
    public int getInitDelaySecs()
    {
        return initDelaySecs;
    }


    @ManagedAttribute
    public int getIntervalSecs()
    {
        return intervalSecs;
    }
}
