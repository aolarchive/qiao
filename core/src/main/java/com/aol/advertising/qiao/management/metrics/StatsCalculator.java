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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
import com.aol.advertising.qiao.util.IntervalMetric;

@ManagedResource(description = "Metrics Calculator")
public class StatsCalculator implements Runnable,
        ApplicationListener<StatsControl>
{
    public enum StatType
    {
        AVERAGE, RAW, DIFF, GAUGE
    };

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private long intervalSecs = 60;
    private long initDelaySecs = 15;
    private String fmtDuration = "Time %1$tMm:%1$tSs|  ";
    private String fmtId = "[%1$.12s] ";
    private boolean writeToLog = false;
    private boolean running = false;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture< ? > schedulerHandler;
    @SuppressWarnings("restriction")
    private com.sun.management.OperatingSystemMXBean osMxBean;
    private MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
    private int availProcessors = 0;

    private Map<String, CalcCallable> statsCalcCallable = new ConcurrentHashMap<String, CalcCallable>();


    @SuppressWarnings("restriction")
    public void init()
    {
        OperatingSystemMXBean bean = ManagementFactory
                .getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean)
        {
            availProcessors = bean.getAvailableProcessors();
            osMxBean = (com.sun.management.OperatingSystemMXBean) bean;
        }

    }


    public void start()
    {
        schedulerHandler = scheduler.scheduleAtFixedRate(this, initDelaySecs,
                intervalSecs, TimeUnit.SECONDS);

        logger.info("stats calculator starts");
        running = true;
    }


    @ManagedOperation(description = "Stop the calculation")
    public void stop()
    {
        if (running)
        {
            schedulerHandler.cancel(false);
            logger.info("stats calculator stops");
            running = false;
        }
    }


    @ManagedOperation(description = "Restart the calculation")
    public void restart()
    {
        if (!running)
        {
            schedulerHandler = scheduler.scheduleAtFixedRate(this,
                    initDelaySecs, intervalSecs, TimeUnit.SECONDS);

            logger.info("stats calculator restarts");
            running = true;
        }
        else
        {
            logger.info("stats calculator already running");
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

        for (Callable< ? > c : statsCalcCallable.values())
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
     * Using Sun's undocumented internal OperatingSystemMXBean is certainly
     * questionable practice for production code. But until there is a better
     * method, it's useful for benchmarking during development.
     *
     * @return the percent CPU time used by the process, or -1 if this operation
     *         is not supported.
     */
    @SuppressWarnings("restriction")
    private double getVmCpu()
    {
        if (osMxBean == null)
            return -1.0;

        double cpu_load = osMxBean.getProcessCpuLoad();
        if (cpu_load < 0)
            return -1.0;

        return cpu_load * 100.0;
    }


    /**
     * @return heap memory used in Mbytes.
     */
    private double getVmHeapMemoryUsed()
    {
        return memoryMxBean.getHeapMemoryUsage().getUsed() / 1000000.0;
    }


    /**
     * Set the interval in seconds to perform calculation.
     *
     * @param intervalSecs
     */
    public void setIntervalSecs(long intervalSecs)
    {
        this.intervalSecs = intervalSecs;
    }


    /**
     * Set whether or not to write the stats to the log file
     *
     * @param writeToLog
     */
    public void setWriteToLog(boolean writeToLog)
    {
        this.writeToLog = writeToLog;
    }


    @ManagedAttribute
    public boolean isRunning()
    {
        return running;
    }


    @ManagedAttribute
    public int getAvailProcessors()
    {
        return availProcessors;
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
            case START_STATS_LOGGING:
                writeToLog = true;
                break;
            case STOP_STATS_LOGGING:
                writeToLog = false;
                break;
            case RESET_STATS:
                resetCounters();
                break;
            default:
                ;
        }
    }


    private void resetCounters()
    {
        for (CalcCallable c : statsCalcCallable.values())
        {
            resetCounters(c);
        }
    }


    private void resetCounters(CalcCallable calcCallable)
    {
        calcCallable.getStatsStore().resetCounters();

        resetCalcCounters(calcCallable);

        logger.info(calcCallable.getId() + "> reset counters to 0s");
    }


    public void resetCalcCounters(CalcCallable calcCallable)
    {
        Map<String, Long> counters = calcCallable.getCounters();
        for (String k : counters.keySet())
        {
            counters.put(k, 0L);
        }
    }


    public void resetCalcCounters()
    {
        for (CalcCallable c : statsCalcCallable.values())
        {
            resetCalcCounters(c);
        }
    }


    /**
     * Register the callable object to be invoked at each interval.
     *
     * @param c
     */
    public void register(CalcCallable c)
    {
        CalcCallable old = statsCalcCallable.get(c.getId());
        if (old == null)
        {
            statsCalcCallable.put(c.getId(), c);
        }
        else
        {
            old.merge(c);
        }
    }


    /**
     * Unregister the previously registered callable object.
     *
     * @param c
     */
    public void unregister(CalcCallable c)
    {
        statsCalcCallable.remove(c.getId());
    }


    @ManagedAttribute
    public boolean isWriteToLog()
    {
        return writeToLog;
    }


    public void setInitDelaySecs(long initDelaySecs)
    {
        this.initDelaySecs = initDelaySecs;
    }


    public void setScheduler(ScheduledExecutorService scheduler)
    {
        this.scheduler = scheduler;
    }

    public class CalcCallable implements Callable<Void>
    {
        private String id;
        private IStatisticsStore statsStore;
        private Map<String, PubStats> counterKeys;
        private Map<String, Long> counters = new ConcurrentHashMap<String, Long>();
        private Map<String, Long> intvlCounters = new ConcurrentHashMap<String, Long>();
        private long prevCalcTime = 0;


        public CalcCallable(IStatisticsStore statsStore,
                Map<String, PubStats> inCounterKeys)
        {
            this.statsStore = statsStore;
            this.counterKeys = inCounterKeys;
            this.prevCalcTime = System.currentTimeMillis();
            String s = statsStore.getId();
            id = (s == null ? "" : String.format(fmtId, s));

            for (String key : inCounterKeys.keySet())
            {
                PubStats pstats = inCounterKeys.get(key);
                if (pstats.getType() == PubStats.StatType.COUNTER)
                {
                    if ((inCounterKeys.get(key).isPubAverage())
                            || (inCounterKeys.get(key).isPubDiff()))
                    {
                        Long value = statsStore.getMetric(key);
                        if (value == null)
                            value = 0L;

                        counters.put(key, value);
                    }
                }
                else if (pstats.getType() == PubStats.StatType.INTERVAL_METRIC)
                {
                    if ((inCounterKeys.get(key).isPubAverage())
                            || (inCounterKeys.get(key).isPubDiff()))
                    {

                        Long value = statsStore.getMetric(key);
                        if (value == null)
                            value = 0L;

                        counters.put(key, value);

                        IntervalMetric im_value = statsStore
                                .getIntervalMetric(key);
                        if (im_value == null)
                            im_value = new IntervalMetric(key, 0, 0);

                        logger.debug(id + " initialize intvlCounter for " + key);

                        intvlCounters.put(key, round(im_value.avg()));
                    }
                }
            }
        }


        private long round(double v)
        {
            return Math.round(v);
        }


        // for tracing
        @SuppressWarnings("unused")
        private void dumpCounterKeys()
        {
            logger.info(id + " >countKeys keyset: "
                    + counterKeys.keySet().toString());
            logger.info(id + " >intvlCounters keyset: "
                    + intvlCounters.keySet().toString());
            logger.info(id + " >statsStores keyset: "
                    + statsStore.getStatsKeySet().toString());
        }


        public void calc()
        {

            StringBuilder sb_avg = new StringBuilder();
            StringBuilder sb_raw = new StringBuilder();
            StringBuilder sb_gauge = new StringBuilder();

            long curr_time = System.currentTimeMillis();
            long duration = curr_time - prevCalcTime;

            for (String key : counterKeys.keySet())
            {
                PubStats pstats = counterKeys.get(key);
                if (pstats.getType() == PubStats.StatType.COUNTER)
                {
                    computeCounterMetric(key, pstats, sb_avg, sb_raw, sb_gauge,
                            duration);
                }
                else if (pstats.getType() == PubStats.StatType.INTERVAL_METRIC)
                {
                    computeIntervalMetric(key, pstats, sb_avg, sb_raw, duration);
                }
                else
                {
                    logger.warn("ignore invalid StatType: " + pstats.getType());
                }
            }

            if (writeToLog)
            {
                String sys = String.format("cpu=%.2f,heap=%.2fMB", getVmCpu(),
                        getVmHeapMemoryUsed());
                String s;
                logger.info((s = id + String.format(fmtDuration, duration))
                        + sys);
                int len = s.length();

                String label = "avg|  ";
                if (sb_avg.length() > 0)
                    logger.info(String.format("%1$-" + (len - label.length())
                            + "s", " ")
                            + label + sb_avg.substring(1));

                label = "counter|  ";
                if (sb_raw.length() > 0)
                    logger.info(String.format("%1$-" + (len - label.length())
                            + "s", " ")
                            + label + sb_raw.substring(1));

                label = "|  ";
                if (sb_gauge.length() > 0)
                    logger.info(String.format("%1$-" + (len - label.length())
                            + "s", " ")
                            + label + sb_gauge.substring(1));
            }

            prevCalcTime = curr_time;

        }


        public void computeCounterMetric(String key, PubStats pstats,
                StringBuilder sbAvg, StringBuilder sbRaw,
                StringBuilder sbGauge, long duration)
        {

            long value = statsStore.getMetric(key);

            if (pstats.isPubAverage() || pstats.isPubDiff())
            {
                long nvalue = value;
                long ovalue = counters.get(key);
                long diff = 0;
                if (nvalue != ovalue)
                    diff = nvalue - ovalue;

                if (pstats.isPubAverage())
                {
                    long avg = 0;
                    if (diff != 0)
                        avg = Math.round(1000.0 * diff / duration); // in secs

                    statsStore.set(StatsEnum.AVERAGE + key, avg);

                    if (writeToLog)
                    {
                        sbAvg.append(String.format(",%s=%d/s", key, avg));
                    }
                }

                if (pstats.isPubDiff())
                {
                    if (writeToLog)
                    {
                        sbRaw.append(String.format(",%s=%d", key, diff));
                    }

                }

                counters.put(key, nvalue);

            }

            if (pstats.isPubRaw())
            {
                if (writeToLog)
                    sbRaw.append(String.format(",tot_%s=%d", key, value));
            }

            if (pstats.isPubGauge())
            {
                int v = pstats.getGaugeValue();
                statsStore.set(key, v);

                if (writeToLog)
                    sbGauge.append(String.format(",%s=%d", key, v));
            }

        }


        public void computeIntervalMetric(String key, PubStats pstats,
                StringBuilder sbAvg, StringBuilder sbRaw, long duration)
        {

            IntervalMetric value = statsStore.getIntervalMetric(key);
            if (value == null)
            {
                logZeros(key, pstats, sbAvg, sbRaw);
                return;
            }

            if (pstats.isPubAverage() || pstats.isPubDiff())
            {
                long new_avg = round(value.avgAndReset()); // current average - in msecs

                if (pstats.isPubAverage())
                {
                    statsStore.set(StatsEnum.AVERAGE + key, new_avg);

                    if (writeToLog)
                    {
                        sbAvg.append(String.format(",%s=%d", key, new_avg));
                    }
                }

                if (pstats.isPubDiff())
                {
                    Long old_avg = intvlCounters.get(key);
                    if (old_avg == null)
                        old_avg = 0L;

                    long avg_diff = round(new_avg - old_avg);
                    if (writeToLog)
                    {
                        sbRaw.append(String.format(",%s=%d", key, avg_diff));
                    }

                }

                intvlCounters.put(key, new_avg);

            }

            if (pstats.isPubRaw())
            {
                if (writeToLog)
                    sbRaw.append(String.format(",tot_%s=%d", key,
                            value.getTime()));
            }

        }


        private void logZeros(String key, PubStats pstats, StringBuilder sbAvg,
                StringBuilder sbRaw)
        {
            if (pstats.isPubAverage())
            {
                if (writeToLog)
                {
                    sbAvg.append(String.format(",%s=0", key));
                }
            }

            if (pstats.isPubDiff())
            {
                if (writeToLog)
                {
                    sbRaw.append(String.format(",%s=0", key));
                }

            }
            if (pstats.isPubRaw())
            {
                if (writeToLog)
                    sbRaw.append(String.format(",tot_%s=0", key));
            }
        }


        @Override
        public Void call() throws Exception
        {
            calc();
            return null;
        }


        public CalcCallable merge(CalcCallable other)
        {
            if (!other.id.equals(this.id))
            {
                logger.warn("cannot merge counterKeys from different stores");
                return null;
            }

            this.counterKeys.putAll(other.counterKeys);
            this.counters.putAll(other.counters);
            this.intvlCounters.putAll(other.intvlCounters);

            return this;
        }


        public IStatisticsStore getStatsStore()
        {
            return statsStore;
        }


        public Map<String, Long> getCounters()
        {
            return counters;
        }


        public String getId()
        {
            return id;
        }
    }
}
