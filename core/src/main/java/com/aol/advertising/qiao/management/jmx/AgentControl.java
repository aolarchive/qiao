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
 * File Name:   NodeControl.java	
 * Description:
 * @author:     ytung
 * @version:    2.0
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management.jmx;

public class AgentControl extends ControlCommand
{
    private static final long serialVersionUID = -3161717308398411329L;


    public AgentControl(Object source, EnumCommand cmd)
    {
        super(source, cmd);
    }


    public AgentControl(Object source, EnumCommand cmd, String payload)
    {
        super(source, cmd, payload);
    }
}
