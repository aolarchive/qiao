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
 * File Name:   SingleSubnodeConfiguration.java	
 * Description:
 * @author:     ytung
 * @version:    1.1
 *
 ****************************************************************************/

package com.aol.advertising.qiao.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Attribute map for a node in an xml configuration file.
 */
public class SingleSubnodeConfiguration extends HashMap<String, Object>
{
    private static final long serialVersionUID = 4659676278541441107L;
    private String id;
    private Map<String, PropertyValue> properties;

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    // @SuppressWarnings("unchecked")
    public Map<String, PropertyValue> getProperties()
    {
        // return (Map<String, PropertyValue>) get(Constants.CFGKEY_PROPERTIES);
        return properties;
    }

    public Object getAttribute(String key)
    {
        return get(key);
    }

    public void setProperties(Map<String, PropertyValue> properties)
    {
        this.properties = properties;
    }
}
