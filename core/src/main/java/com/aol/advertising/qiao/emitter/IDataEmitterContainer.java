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

import java.util.List;
import java.util.Map;

import com.aol.advertising.qiao.agent.IDataPipe;
import com.aol.advertising.qiao.event.EventWrapper;
import com.lmax.disruptor.EventHandler;

public interface IDataEmitterContainer extends IDataEmitter,
        EventHandler<EventWrapper>
{
    public void setDataEmitterList(List<IDataEmitter> emitterList);


    public void setIdEmitterMap(Map<String, IDataEmitter> idMap);


    public void setDataPipe(IDataPipe dataPipe);


    public void drainThenSuspend();


    public void setRateLimit(int targetLimitedRate);


    public void changeRateLimit(int targetLimitedRate);

}
