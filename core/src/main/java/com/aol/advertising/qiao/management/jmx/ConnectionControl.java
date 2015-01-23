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
 * File Name:   ConnectionControl.java	
 * Description:
 * @author:     ytung
 * @version:    2.0
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management.jmx;

public class ConnectionControl extends ControlCommand
{

    private static final long serialVersionUID = 1381822375099761804L;


    ConnectionControl(Object source, EnumCommand cmd)
    {
        super(source, cmd);
    }


    ConnectionControl(Object source, EnumCommand cmd, String payload)
    {
        super(source, cmd, payload);
    }
}
