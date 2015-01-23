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
 * File Name:   MultiSubnodeConfiguration.java	
 * Description:
 * @author:     ytung
 * @version:    1.1
 *
 ****************************************************************************/

package com.aol.advertising.qiao.config;

import java.util.ArrayList;
import java.util.Map;

public class MultiSubnodeConfiguration extends
        ArrayList<SingleSubnodeConfiguration>
{
    private static final long serialVersionUID = -7541390650412911504L;
    private String id;
    private Map<String, String> properties;


    public String getId()
    {
        return id;
    }


    public void setId(String id)
    {
        this.id = id;
    }


    public Map<String, String> getProperties(int index)
    {
        return properties;
    }
}
