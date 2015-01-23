/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2014 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   DefaultKafkaStringMessageHandler.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter.handler;

import java.util.concurrent.atomic.AtomicLong;

import com.aol.advertising.qiao.event.IKafkaEvent;

public class DefaultKafkaStringMessageHandler implements
        IMessageHandler<Object, IKafkaEvent<Long, String>>
{
    private AtomicLong sequenceNumber = new AtomicLong(0);


    @Override
    public IKafkaEvent<Long, String> handle(final Object event)
    {
        if (!(event instanceof String))
            return null;

        final String str_event = (String) event;

        return new IKafkaEvent<Long, String>()
        {

            @Override
            public Long getKey()
            {
                return sequenceNumber.incrementAndGet();
            }


            @Override
            public String getMessage()
            {
                return str_event;
            }
        };
    }

}
