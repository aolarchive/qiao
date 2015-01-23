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
 * File Name:   EventWrapper.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.event;

import com.lmax.disruptor.EventFactory;

public class EventWrapper
{
    private Object event;


    public Object getEvent()
    {
        return event;
    }


    public void setEvent(Object event)
    {
        this.event = event;
    }


    public String toString()
    {
        return event.toString();
    }

    public final static EventFactory<EventWrapper> EVENT_FACTORY = new EventFactory<EventWrapper>()
    {

        @Override
        public EventWrapper newInstance()
        {
            return new EventWrapper();
        }

    };

}
