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
 * File Name:   AppControl.java	
 * Description:
 * @author:     ytung
 * @version:    1.1
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management.jmx;

public class AppControl extends ControlCommand
{
    private static final long serialVersionUID = -8077959470539065801L;

    AppControl(Object source, EnumCommand cmd, String payload)
    {
        super(source, cmd, payload);
    }

}
