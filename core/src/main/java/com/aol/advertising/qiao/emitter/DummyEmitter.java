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

package com.aol.advertising.qiao.emitter;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.management.metrics.StatsCollector;

/**
 * Internal use only.
 */
@ManagedResource
public class DummyEmitter extends AbstractDataEmitter
{
    private AtomicLong numOutput = new AtomicLong(0);


    @Override
    public void process(Object data)
    {
        numOutput.incrementAndGet();
    }


    @Override
    public void removeThreadLocal()
    {
    }


    @Override
    public void setStatsCollector(StatsCollector statsCollector)
    {
    }


    @Override
    public void setEmitterThreadCount(int threadCount)
    {
    }

}
