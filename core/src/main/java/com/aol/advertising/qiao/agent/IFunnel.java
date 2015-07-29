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

package com.aol.advertising.qiao.agent;

import com.aol.advertising.qiao.emitter.IDataEmitterContainer;
import com.aol.advertising.qiao.injector.IDataInjector;
import com.aol.advertising.qiao.management.IStatsCalculatorAware;
import com.aol.advertising.qiao.management.IStatsCollectable;
import com.aol.advertising.qiao.management.metrics.IStatisticsStore;

public interface IFunnel extends IStatsCollectable, IStatsCalculatorAware
{
    public void setDataInjector(IDataInjector src);


    public void setDataEmitter(IDataEmitterContainer sink);


    public void init() throws Exception;


    public void start() throws Exception;


    public void close();


    public String getId();


    public void setId(String id);;


    public void setEmitterThreadCount(int sinkThreadCount);


    public void setDataPipeCapacity(int dataPipeCapacity);


    public boolean isRunning();


    public void setStatsStore(IStatisticsStore statsStore);


    public void suspend();


    public void drainThenSuspend();


    public void resume();


    public void setAutoStart(boolean autoStart);


    public void setRateLimit(int targetLimitedRate);

}
