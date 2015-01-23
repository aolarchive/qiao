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
 * File Name:   DefaultAMQMessageCreator.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter.handler;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * DefaultAMQMessageCreator is a factory class that creates a JMS message object
 * based on event type. Supported event types are String and Serializable. For
 * each event object, it create a JMS TextMessage if the object is String type,
 * or ObjectMessage if the event object implements Serializable.
 */
public class DefaultAMQMessageCreator implements IJmsMessageCreator<Object>
{

    /**
     * Creates a JMS message object based on event type. If the event argument
     * is of type String, this creates and returns an initialized TextMessage
     * object. If it is a Serializable object, it returns an initialized
     * ObjectMessage object.
     */
    @Override
    public Message createMessage(Session session, Object event)
            throws JMSException
    {
        if (event instanceof String)
        {
            return session.createTextMessage((String) event);
        }
        else if (event instanceof Serializable)
        {
            return session.createObjectMessage((Serializable) event);
        }

        return null;
    }

}
