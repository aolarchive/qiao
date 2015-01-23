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
 * File Name:   PubStats.java	
 * Description:
 * @author:     ytung05
 *
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
