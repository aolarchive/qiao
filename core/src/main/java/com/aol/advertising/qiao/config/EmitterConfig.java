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
 * File Name:   EmitterConfig.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.config;

import java.util.Map;

public class EmitterConfig
{
    protected String emitterContainerClassName;
    protected Map<String, String> emitterClassNames; // <id, class>
    protected MultiSubnodeConfiguration emitterConfig;


    public String getEmitterContainerClassName()
    {
        return emitterContainerClassName;
    }


    public void setEmitterContainerClassName(String sinkContainerClassName)
    {
        this.emitterContainerClassName = sinkContainerClassName;
    }


    public Map<String, String> getEmitterClassNames()
    {
        return emitterClassNames;
    }


    public void setEmitterClassNames(Map<String, String> sinkClassNames)
    {
        this.emitterClassNames = sinkClassNames;
    }


    public MultiSubnodeConfiguration getSinkConfig()
    {
        return emitterConfig;
    }


    public void setEmitterConfig(MultiSubnodeConfiguration sinkConfig)
    {
        this.emitterConfig = sinkConfig;
    }

}
