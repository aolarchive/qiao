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
 * File Name:   DataSpray.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.aol.advertising.qiao.agent.DataDisruptor;
import com.aol.advertising.qiao.agent.IDataPipe;
import com.aol.advertising.qiao.event.EventWrapper;
import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.management.IStatsCalculatorAware;
import com.aol.advertising.qiao.management.IStatsCollectable;
import com.aol.advertising.qiao.management.metrics.IStatisticsStore;
import com.aol.advertising.qiao.management.metrics.PubStats;
import com.aol.advertising.qiao.management.metrics.PubStats.StatType;
import com.aol.advertising.qiao.management.metrics.StatsCalculator;
import com.aol.advertising.qiao.management.metrics.StatsCollector;
import com.aol.advertising.qiao.management.metrics.StatsEnum;
import com.aol.advertising.qiao.management.metrics.StatsEvent;
import com.aol.advertising.qiao.management.metrics.StatsEvent.StatsOp;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.IntervalMetric;
import com.aol.advertising.qiao.util.RateLimiter;
import com.aol.advertising.qiao.util.StatsUtils;

@ManagedResource
public class DataSpray implements IDataEmitterContainer, //Runnable,
        IStatsCollectable, IStatsCalculatorAware
{
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected List<IDataEmitter> emitterList;
    protected Map<String, IDataEmitter> idMap;

    protected IDataPipe pipe;

    protected String id = "sray"; // overridden by setFunnelId
    protected String funnelId = "funnel";
    protected ThreadPoolTaskExecutor executor;
    protected int threadCount = 1;
    protected volatile boolean running = false;
    protected Runnable runnable;

    protected AtomicLong emitted = new AtomicLong(0);
    protected ApplicationEventPublisher eventPublisher;
    protected StatsCollector statsCollector;
    protected long maxWaitTimeMillis = 500;

    private String statKeyDispatchTime = "spray_" + StatsEnum.EMIT_TIME.value()
            + "_nano";
    private IntervalMetric stats;
    protected StatsCalculator statsCalculator;

    protected DataDisruptor disruptor; // only if data pipe is of type DataDisruptor
    protected AtomicBoolean isSuspended = new AtomicBoolean(false);

    protected int targetLimitedRate = 0;
    protected RateLimiter rateLimiter;


    @Override
    public void init() throws Exception
    {
        _registerStatsCollector();
        _registerStatsCalculator();

        if (pipe instanceof DataDisruptor)
        {
            disruptor = (DataDisruptor) pipe;
            disruptor.setThreadPoolSize(threadCount);
            disruptor.addEventHandler(this);

           // threadCount = 0;
        }
        else
        {
            executor = CommonUtils.createFixedThreadPoolExecutor(threadCount);
            executor.setThreadNamePrefix(CommonUtils.resolveThreadName(id));
            executor.initialize();
        }

        if (targetLimitedRate > 0)
            rateLimiter = RateLimiter.create(targetLimitedRate);

        for (IDataEmitter emitter : emitterList)
        {
            emitter.setEmitterThreadCount(threadCount);
            emitter.setApplicationEventPublisher(eventPublisher);
            emitter.init();
        }

    }


    private void _registerStatsCollector()
    {
        stats = new IntervalMetric(statKeyDispatchTime);

        final String clzname = this.getClass().getSimpleName();

        if (statsCollector != null)
        {
            statsCollector.register(new Callable<Void>()
            {
                @Override
                public Void call()
                {
                    if (emitted.get() > 0)
                        eventPublisher.publishEvent(new StatsEvent(this,
                                clzname, funnelId, StatsOp.INCRBY,
                                StatsEnum.EVENT_OUTPUT_COUNT.value(), emitted
                                        .getAndSet(0)));

                    eventPublisher.publishEvent(new StatsEvent(this, clzname,
                            funnelId, StatsOp.INCRBY_INTVL_STAT,
                            statKeyDispatchTime, stats.getAndReset()));

                    return null;
                }

            });

        }

    }


    private void _registerStatsCalculator()
    {
        if (statsCalculator != null)
        {
            IStatisticsStore statsStore = StatsUtils.getStatsStore(funnelId);
            if (statsStore == null)
                throw new ConfigurationException(funnelId
                        + " statistics store does not exist");

            Map<String, PubStats> counter_keys = new LinkedHashMap<String, PubStats>();

            PubStats pstats = new PubStats(StatType.INTERVAL_METRIC,
                    statKeyDispatchTime, false, true, false, false);
            counter_keys.put(pstats.getMetric(), pstats);

            statsCalculator.register(statsCalculator.new CalcCallable(
                    statsStore, counter_keys));
        }

    }


    @Override
    public void start() throws Exception
    {
        running = true;

        for (IDataEmitter emitter : emitterList)
        {
            emitter.start();
        }

        if (!(pipe instanceof DataDisruptor))
        {
            runnable = new SprayRunnable();
            for (int i = 0; i < threadCount; i++)
            {
                executor.submit(runnable);
            }
        }

    }


    @Override
    public void shutdown()
    {
        if (!running)
            return;

        long wtime = this.maxWaitTimeMillis;
        if (!pipe.isEmpty())
        {
            do
            {
                logger.info("waiting for data to drain... (size=" + pipe.size()
                        + ")");
                CommonUtils.sleepQuietly(100);
                wtime -= 100;
            }
            while (!pipe.isEmpty() && wtime > 0);
        }

        if (pipe instanceof DataDisruptor)
        {
            disruptor.shutdown();
        }

        for (IDataEmitter emitter : emitterList)
            emitter.shutdown();

        if (executor != null)
            executor.destroy();

        if (rateLimiter != null)
            rateLimiter.destroy();

        running = false;

    }


    @Override
    public void removeThreadLocal()
    {
        for (IDataEmitter emitter : emitterList)
            emitter.removeThreadLocal();
    }


    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher publisher)
    {
        this.eventPublisher = publisher;

    }


    @Override
    public void setDataEmitterList(List<IDataEmitter> emitterList)
    {
        this.emitterList = emitterList;
    }


    @Override
    public void setIdEmitterMap(Map<String, IDataEmitter> idMap)
    {
        this.idMap = idMap;
    }


    @Override
    public void setDataPipe(IDataPipe dataPipe)
    {
        this.pipe = dataPipe;
    }


    @Override
    public void process(Object o)
    {
        long ts_start = System.nanoTime();

        // broadcast
        for (IDataEmitter emitter : emitterList)
            emitter.process(o);

        long dur = System.nanoTime() - ts_start;
        stats.update(dur);
    }


    @Override
    public void setFunnelId(String id)
    {
        this.funnelId = id;
        this.setId("DataSpray-" + this.funnelId);
    }


    @ManagedAttribute
    @Override
    public String getId()
    {
        return id;
    }


    @ManagedAttribute
    public long getEmittedCount()
    {
        return emitted.get();
    }


    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            if (pipe instanceof DataDisruptor)
            {
                disruptor.suspendEventHandlers();
            }
            else
            {
                executor.destroy();
            }

            for (IDataEmitter emitter : emitterList)
            {
                emitter.suspend();
            }
        }
        else
            logger.warn("No. Already suspended.");
    }


    @Override
    public void drainThenSuspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            logger.info("draining the internal queue before suspending...");

            while (!pipe.isEmpty())
                CommonUtils.sleepQuietly(10);

            logger.info("queue size: " + pipe.size());
            if (pipe instanceof DataDisruptor)
            {
                disruptor.suspendEventHandlers();
            }
            else
            {
                executor.destroy();
            }
        }
        else
            logger.warn("No. Already suspended.");
    }


    public void resume()
    {
        if (isSuspended.compareAndSet(true, false))
        {
            for (IDataEmitter emitter : emitterList)
            {
                emitter.resume();
            }

            if (pipe instanceof DataDisruptor)
            {
                disruptor.resumeEventHandlers();
            }
            else
            {
                executor = CommonUtils
                        .createFixedThreadPoolExecutor(threadCount);
                executor.setThreadNamePrefix(id);
                executor.initialize();

                for (int i = 0; i < threadCount; i++)
                {
                    executor.submit(runnable);
                }
            }
        }
        else
            logger.warn("No. Not in suspend mode.");
    }


    @Override
    public void setStatsCollector(StatsCollector statsCollector)
    {
        this.statsCollector = statsCollector;
    }


    @Override
    public void setId(String id)
    {
        this.id = id;

    }


    /**
     * Only apply to DataDisruptor data pipe.
     * 
     * @see com.lmax.disruptor.EventHandler#onEvent(java.lang.Object, long,
     *      boolean)
     */
    @Override
    public void onEvent(EventWrapper event, long sequence, boolean endOfBatch)
            throws Exception
    {
        disruptor.incrementAndGet();

        process(event.getEvent());

        emitted.incrementAndGet();

    }

    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    public class SprayRunnable implements Runnable
    {
        @Override
        public void run()
        {
            while (running)
            {
                try
                {
                    Object o = pipe.read();
                    if (o == null)
                    {
                        logger.info("pipe read returns null");
                        continue;
                    }

                    _pacing();

                    process(o);

                    emitted.incrementAndGet();

                }
                catch (InterruptedException e)
                {
                    break;
                }
                catch (Exception e)
                {
                    logger.error(e.getMessage(), e);
                }
            }

            removeThreadLocal();

            logger.info(this.getClass().getSimpleName() + " thread terminated");
        }

    }


    protected void _pacing()
    {
        if (rateLimiter != null)
        {
            try
            {
                rateLimiter.acquire();
            }
            catch (InterruptedException e)
            {
            }
        }
    }


    @Override
    public void setRateLimit(int targetLimitedRate)
    {
        this.targetLimitedRate = targetLimitedRate;
    }


    public void changeRateLimit(int newRate)
    {
        logger.info("changing rate limit from " + this.targetLimitedRate
                + " to " + newRate);
        this.targetLimitedRate = newRate;
        if (rateLimiter != null)
        {
            if (newRate > 0)
            {
                rateLimiter.modifyRate(newRate);
            }
            else
            {
                rateLimiter.destroy();
                rateLimiter = null;
            }
        }
        else
        {
            if (targetLimitedRate > 0)
                rateLimiter = RateLimiter.create(targetLimitedRate);
        }

    }


    @Override
    public boolean isRunning()
    {
        boolean is_running = running
                && (executor != null ? executor.getActiveCount() > 0 : true);
        if (!is_running)
            return false;

        for (IDataEmitter emitter : emitterList)
        {
            if (!emitter.isRunning())
            {
                is_running = false;
                break;
            }
        }

        return is_running;
    }


    @Override
    public boolean isSuspended()
    {
        return isSuspended.get();
    }


    public void setStatKeyDispatchTime(String statKeyDispatchTime)
    {
        this.statKeyDispatchTime = statKeyDispatchTime;
    }


    @Override
    public void setStatsCalculator(StatsCalculator statsCalculator)
    {
        this.statsCalculator = statsCalculator;
    }


    @Override
    public void setEmitterThreadCount(int threadCount)
    {
        this.threadCount = threadCount;
    }

}
