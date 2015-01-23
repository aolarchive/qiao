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
 * File Name:   IDataEmitterContainer.java	
 * Description:
 * @author:     ytung05
 *
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
