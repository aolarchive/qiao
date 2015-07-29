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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.support.MetricType;

import com.aol.advertising.qiao.util.IntervalMetric;

@ManagedResource
public class StatisticsStore implements IStatisticsStore
{

    protected Map<String, AtomicLong> stats = new HashMap<String, AtomicLong>();
    protected Map<String, AtomicReference<IntervalMetric>> intvalStats = new HashMap<String, AtomicReference<IntervalMetric>>();
    protected boolean running;
    protected String id;
    protected StatsManager statsManager;


    @Override
    public void init() throws Exception
    {
        statsManager.addStatsStore(id, this);

        // initialize predefined counters
        for (StatsEnum t : StatsEnum.values())
        {
            stats.put(t.value(), new AtomicLong(0));
        }
    }


    @ManagedMetric(metricType = MetricType.COUNTER, description = "Input event count")
    public long getInputCount()
    {
        return stats.get(StatsEnum.EVENT_INPUT_COUNT.value()).get();
    }


    @ManagedMetric(metricType = MetricType.COUNTER, description = "Output event count")
    public long getOutputCount()
    {
        return stats.get(StatsEnum.EVENT_OUTPUT_COUNT.value()).get();
    }


    @ManagedMetric(metricType = MetricType.COUNTER, description = "Processed event count")
    public long getProcessedCount()
    {
        return stats.get(StatsEnum.PROCESSED_COUNT.value()).get();
    }


    @ManagedMetric(metricType = MetricType.COUNTER, description = "Average number of input events in an interval")
    public long getAvgInputCount_LastInterval()
    {
        return stats.get(StatsEnum.INTVL_AVG_INPUT.value()).get();
    }


    @ManagedMetric(metricType = MetricType.COUNTER, description = "Average number of output events in an interval")
    public long getAvgOutputCount_LastInterval()
    {
        return stats.get(StatsEnum.INTVL_AVG_OUTPUT.value()).get();
    }


    @ManagedMetric(metricType = MetricType.COUNTER, description = "Average number of events processed in an interval")
    public long getAvgProcessedCount_LastInterval()
    {
        return stats.get(StatsEnum.INTVL_AVG_PROCESSED.value()).get();
    }


    @ManagedMetric(metricType = MetricType.GAUGE, description = "Data pipe queue depth")
    public long getQueueLength()
    {
        return stats.get(StatsEnum.DATAPIPE_QUEUELEN.value()).get();
    }


    @ManagedMetric(metricType = MetricType.COUNTER, description = "Total processed files")
    public long getProcessedFiles()
    {
        return stats.get(StatsEnum.PROCESSED_FILES.value()).get();
    }


    @ManagedMetric(metricType = MetricType.COUNTER, description = "Total processed data blocks")
    public long getProcessedBlocks()
    {
        return stats.get(StatsEnum.PROCESSED_BLOCKS.value()).get();
    }


    @Override
    public void set(String key, long value)
    {
        if (stats.containsKey(key))
        {
            stats.get(key).set(value);
        }
        else
        {
            stats.put(key, new AtomicLong(0));
        }
    }


    @Override
    @ManagedOperation(description = "Get the specific metric")
    @ManagedOperationParameters({ @ManagedOperationParameter(name = "Metric identifier", description = "The type of metric for value retrieval, e.g. input, output, processed, etc.") })
    public long getMetric(String key)
    {
        AtomicLong v = stats.get(key);
        if (v == null)
            return 0;

        return v.get();
    }


    @Override
    @ManagedOperation(description = "Reset counters")
    public void resetCounters()
    {
        for (AtomicLong v : stats.values())
        {
            v.set(0);
        }

    }


    public void incr(String key, long delta)
    {
        if (stats.containsKey(key))
        {
            stats.get(key).addAndGet(delta);
        }
        else
        {
            stats.put(key, new AtomicLong(delta));
        }

    }


    public void incr(String key)
    {
        if (stats.containsKey(key))
        {
            stats.get(key).incrementAndGet();
        }
        else
        {
            stats.put(key, new AtomicLong(1));
        }
    }


    public void decr(String key)
    {
        if (stats.containsKey(key))
        {
            stats.get(key).decrementAndGet();
        }
        // else do nothing
    }


    public void decr(String key, long value)
    {
        if (stats.containsKey(key))
        {
            stats.get(key).addAndGet(-value);
        }
        // else do nothing
    }


    @Override
    public void onStatsEvent(StatsEvent event)
    {

        String key = event.getStatKey();
        switch (event.getStatOp())
        {
            case INCR:
                incr(key);
                break;
            case DECR:
                decr(key);
                break;
            case INCRBY:
                incr(key, event.getStatValue());
                break;
            case DECRBY:
                decr(key, event.getStatValue());
                break;
            case SET:
                set(key, event.getStatValue());
                break;
            case INCRBY_INTVL_STAT:
                incr(key, event.getStatIMetric());
                break;
            case SET_INTVL_STAT:
                set(key, event.getStatIMetric());
                break;
            case RESET:
                resetCounters();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operation: " + event.getStatOp());
        }
    }


    @Override
    public void setId(String id)
    {
        this.id = id;
    }


    @Override
    public String getId()
    {
        return id;
    }


    public void setStatsManager(StatsManager statsManager)
    {
        this.statsManager = statsManager;
    }


    public Set<String> getStatsKeySet()
    {
        return stats.keySet();
    }


    @ManagedOperation
    public String getStatsKeys()
    {
        return stats.keySet().toString();
    }


    @ManagedOperation
    public String getDoubleStatsKeys()
    {
        return intvalStats.keySet().toString();
    }


    /**
     * @param key
     * @param imetric
     */
    public void setIntervalMetric(String key, IntervalMetric value)
    {
        intvalStats.put(key, new AtomicReference<IntervalMetric>(value));
    }


    public void updateIntervalMetric(String key, IntervalMetric value)
    {
        if (intvalStats.containsKey(key))
        {
            intvalStats.get(key).get().incr(value);
        }
        else
        {
            intvalStats.put(key, new AtomicReference<IntervalMetric>(value));
        }
    }


    @ManagedOperation(description = "Get the specific metric")
    public double getAverageIntervalMetric(String key)
    {
        AtomicReference<IntervalMetric> v = intvalStats.get(key);
        if (v == null)
            return 0;

        return v.get().avg();
    }


    @Override
    public IntervalMetric getIntervalMetric(String key)
    {
        AtomicReference<IntervalMetric> v = intvalStats.get(key);
        if (v == null)
            return null;

        return v.get();
    }


    public void incr(String key, IntervalMetric delta)
    {
        if (intvalStats.containsKey(key))
        {
            intvalStats.get(key).get().incr(delta);
        }
        else
        {
            intvalStats.put(key, new AtomicReference<IntervalMetric>(delta));
        }

    }


    public void set(String key, IntervalMetric stats)
    {
        if (intvalStats.containsKey(key))
        {
            intvalStats.get(key).set(stats);
        }
        else
        {
            intvalStats.put(key, new AtomicReference<IntervalMetric>(stats));
        }

    }
}
