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
 * File Name:   InjectorConfig.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.config;

public class InjectorConfig
{
    protected String sourceClassName;
    protected String id;
    protected SingleSubnodeConfiguration sourceConfig;


    public String getSourceClassName()
    {
        return sourceClassName;
    }


    public void setSourceClassName(String sourceClassName)
    {
        this.sourceClassName = sourceClassName;
    }


    public String getId()
    {
        return id;
    }


    public void setId(String id)
    {
        this.id = id;
    }


    public SingleSubnodeConfiguration getSourceConfig()
    {
        return sourceConfig;
    }


    public void setSourceConfig(SingleSubnodeConfiguration sourceConfig)
    {
        this.sourceConfig = sourceConfig;
    }

}
