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
