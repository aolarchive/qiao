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

import java.util.concurrent.Callable;

public class PubStats
{

    public static enum StatType
    {
        COUNTER, INTERVAL_METRIC
    }

    private StatType type;
    private String metric;
    private boolean pubRaw;
    private boolean pubAverage;
    private boolean pubDiff;
    private boolean pubGauge;

    private Callable<Integer> gauge;


    public PubStats(String metric, boolean pubRaw, boolean pubAverage,
            boolean pubDiff, boolean pubGauge)
    {
        this.type = StatType.COUNTER;
        this.metric = metric;
        this.pubRaw = pubRaw;
        this.pubAverage = pubAverage;
        this.pubDiff = pubDiff;
        this.pubGauge = pubGauge;
    }


    public PubStats(StatType type, String metric, boolean pubRaw,
            boolean pubAverage, boolean pubDiff, boolean pubGauge)
    {
        this.type = type;
        this.metric = metric;
        this.pubRaw = pubRaw;
        this.pubAverage = pubAverage;
        this.pubDiff = pubDiff;
        this.pubGauge = pubGauge;
    }


    public String getMetric()
    {
        return metric;
    }


    public boolean isPubRaw()
    {
        return pubRaw;
    }


    public boolean isPubAverage()
    {
        return pubAverage;
    }


    public boolean isPubDiff()
    {
        return pubDiff;
    }


    public boolean isPubGauge()
    {
        return pubGauge;
    }


    public void setGauge(Callable<Integer> gauge)
    {
        this.gauge = gauge;
    }


    public int getGaugeValue()
    {
        try
        {
            return gauge.call();
        }
        catch (Exception e)
        {
        }

        return -1;
    }


    public StatType getType()
    {
        return type;
    }
}
