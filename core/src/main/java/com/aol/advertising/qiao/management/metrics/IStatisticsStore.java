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
