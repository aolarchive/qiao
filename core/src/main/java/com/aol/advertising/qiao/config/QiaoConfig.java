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
 * File Name:   FunnelConfig.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.config;

import java.util.Map;

public class QiaoConfig
{
    protected MultiSubnodeConfiguration funnelConfig;
    protected Map<String, String> funnelClassNames; // <id, class>
    protected Map<String, FunnelComponents> funnelComponents;


    public MultiSubnodeConfiguration getFunnelConfig()
    {
        return funnelConfig;
    }


    public void setFunnelConfig(MultiSubnodeConfiguration funnelConfig)
    {
        this.funnelConfig = funnelConfig;
    }


    public Map<String, String> getFunnelClassNames()
    {
        return funnelClassNames;
    }


    public void setFunnelClassNames(Map<String, String> funnelClassNames)
    {
        this.funnelClassNames = funnelClassNames;
    }


    public Map<String, FunnelComponents> getFunnelComponents()
    {
        return funnelComponents;
    }


    public void setFunnelComponents(
            Map<String, FunnelComponents> funnelComponents)
    {
        this.funnelComponents = funnelComponents;
    }

    public class FunnelComponents
    {
        private String id;
        private InjectorConfig sourceConfig;
        private EmitterConfig sinkConfig;


        public String getId()
        {
            return id;
        }


        public void setId(String id)
        {
            this.id = id;
        }


        public InjectorConfig getSourceConfig()
        {
            return sourceConfig;
        }


        public void setSourceConfig(InjectorConfig sourceConfig)
        {
            this.sourceConfig = sourceConfig;
        }


        public EmitterConfig getSinkConfig()
        {
            return sinkConfig;
        }


        public void setSinkConfig(EmitterConfig sinkConfig)
        {
            this.sinkConfig = sinkConfig;
        }
    }

}
