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
 * File Name:   StatsUtils.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.util;

import com.aol.advertising.qiao.management.metrics.IStatisticsStore;
import com.aol.advertising.qiao.management.metrics.StatsManager;

public class StatsUtils
{
    private static StatsUtils _instance;
    private static StatsManager statsManager;


    public static StatsUtils getInstance()
    {
        if (_instance == null)
            _instance = new StatsUtils();

        return _instance;
    }


    public void setStatsManager(StatsManager statsManager)
    {
        StatsUtils.statsManager = statsManager;
    }


    public static long getCounter(String funnelId, String statKey)
    {
        IStatisticsStore ss = statsManager.getStatsStore(funnelId);
        if (ss == null)
            return 0;

        return ss.getMetric(statKey);

    }


    public static IStatisticsStore getStatsStore(String funnelId)
    {
        return statsManager.getStatsStore(funnelId);
    }
}
