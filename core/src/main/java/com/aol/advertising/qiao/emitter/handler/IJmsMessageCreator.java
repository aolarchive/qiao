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
 * File Name:   IJmsMessageCreator.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter.handler;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

public interface IJmsMessageCreator<T>
{
    Message createMessage(Session session, T event) throws JMSException;
}
