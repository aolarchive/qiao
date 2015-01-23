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
 * File Name:   IStatisticsStore.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management.metrics;

import java.util.Set;

import com.aol.advertising.qiao.util.IntervalMetric;

public interface IStatisticsStore
{
    public void init() throws Exception;


    public void set(String key, long value);


    public long getMetric(String key);


    public void resetCounters();


    public void incr(String key, long delta);


    public void decr(String key, long value);


    public void setId(String id);


    public String getId();


    public void onStatsEvent(StatsEvent event);


    public IntervalMetric getIntervalMetric(String key);


    public Set<String> getStatsKeySet();

}
