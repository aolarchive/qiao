/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2011-2012 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   ConnectionController.java	
 * Description:
 * @author:     ytung
 * @version:    2.0
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.context.ApplicationListener;

import com.aol.advertising.qiao.management.jmx.ConnectionControl;

/**
 * All non-singleton beans that need to listen to ConnectionControl commands
 * must register to this class in order to be notified. This is required because
 * Spring container does not hold on instances of prototype beans. Thus an
 * instantiated prototype bean which implements ApplicationListener will never
 * be notified of any published events.
 */
public class ConnectionController implements
        ApplicationListener<ConnectionControl>
{
    public enum ConnectionType
    {
        INBOUND, OUTBOUND
    };

    protected List<IConnectionControlListener> inboundListeners = Collections
            .synchronizedList(new ArrayList<IConnectionControlListener>());;

    protected List<IConnectionControlListener> outboundListeners = Collections
            .synchronizedList(new ArrayList<IConnectionControlListener>());;


    public void register(IConnectionControlListener listener,
            ConnectionType type)
    {
        if (type == ConnectionType.INBOUND)
        {
            inboundListeners.add(listener);
        }
        else
        {
            outboundListeners.add(listener);
        }
    }


    public void unregister(IConnectionControlListener listener,
            ConnectionType type)
    {
        if (type == ConnectionType.INBOUND)
        {
            inboundListeners.remove(listener);
        }
        else
        {
            outboundListeners.remove(listener);
        }
    }


    @Override
    public void onApplicationEvent(ConnectionControl event)
    {
        switch (event.getCommand())
        {
            case RESET_INBOUND_CONNECTION:
                synchronized (inboundListeners)
                {
                    Iterator<IConnectionControlListener> iter = inboundListeners
                            .iterator();
                    while (iter.hasNext())
                    {
                        iter.next().reset();
                    }
                }
                break;
            case RESET_OUTBOUND_CONNECTION:
                synchronized (outboundListeners)
                {
                    Iterator<IConnectionControlListener> iter = outboundListeners
                            .iterator();
                    while (iter.hasNext())
                    {
                        iter.next().reset();
                    }
                }
                break;
            default:
        }
    }

}
