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
 * File Name:   DynamicMBeanExporter.java	
 * Description:
 * @author:     ytung
 * @version:    1.1
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management.jmx;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.BeansException;
import org.springframework.jmx.export.MBeanExporter;
import org.springframework.jmx.export.metadata.JmxAttributeSource;
import org.springframework.jmx.export.metadata.ManagedResource;

import com.aol.advertising.qiao.config.ConfigConstants;
import com.aol.advertising.qiao.util.ContextUtils;

public class DynamicMBeanExporter
{
    private String jmxCommonMBeanMapId = ConfigConstants.MBEAN_COMMON_MAP_ID;
    private String jmxApplicationMBeanMapId = ConfigConstants.MBEAN_APPLICATION_MAP_ID;

    private MBeanExporter exporter;
    private JmxAttributeSource attributeSource;
    private Map<String, Object> mbeanMap;
    private String mbeanDomain;


    public void init() throws BeansException, ClassNotFoundException
    {
        mbeanMap = (Map<String, Object>) ContextUtils
                .loadClassById(jmxCommonMBeanMapId);

        try
        {
            Map<String, Object> app_beans = (Map<String, Object>) ContextUtils
                    .loadClassById(jmxApplicationMBeanMapId);
            if (app_beans != null)
            {
                for (Entry<String, Object> entry : app_beans.entrySet())
                {
                    Object dsp = entry.getValue();
                    ManagedResource mr = attributeSource.getManagedResource(dsp
                            .getClass());
                    if (mr != null)
                        mbeanMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        catch (BeansException e)
        {
            // no application specific mbeans defined
        }

    }


    public void loadMBeans(Map<String, Object> objectMap)
    {

        for (Entry<String, Object> entry : objectMap.entrySet())
        {
            Object dsp = entry.getValue();
            ManagedResource mr = attributeSource.getManagedResource(dsp
                    .getClass());
            if (mr != null)
                mbeanMap.put(entry.getKey(), entry.getValue());
        }

        // set mbeans and load
        exporter.setBeans(mbeanMap);
        exporter.afterPropertiesSet();
    }


    public void destroy()
    {
        exporter.destroy();
    }


    public MBeanExporter getExporter()
    {
        return exporter;
    }


    public JmxAttributeSource getAttributeSource()
    {
        return attributeSource;
    }


    public void setJmxCommonMBeanMapId(String jmxCommonMBeanMapId)
    {
        this.jmxCommonMBeanMapId = jmxCommonMBeanMapId;
    }


    public void setMbeanDomain(String mbeanDomain)
    {
        this.mbeanDomain = mbeanDomain;
    }


    public void setJmxApplicationMBeanMapId(String jmxApplicationMBeanMapId)
    {
        this.jmxApplicationMBeanMapId = jmxApplicationMBeanMapId;
    }


    public void setExporter(MBeanExporter exporter)
    {
        this.exporter = exporter;
    }


    public void setAttributeSource(JmxAttributeSource attributeSource)
    {
        this.attributeSource = attributeSource;
    }
}
