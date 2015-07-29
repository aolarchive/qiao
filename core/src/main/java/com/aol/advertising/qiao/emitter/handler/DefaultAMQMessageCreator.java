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
