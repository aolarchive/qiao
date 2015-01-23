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
 * File Name:   ConfigConstants.java	
 * Description:
 * @author:     ytung
 *
 ****************************************************************************/

package com.aol.advertising.qiao.config;

public interface ConfigConstants
{
    public static final String PROP_QIAO_CFG_DIR = "qiao.config.dir";

    public static final String CFGKEY_FUNNEL = "funnel";
    public static final String CFGKEY_FUNNEL_INJECTOR = "funnel(%d).injector";
    public static final String CFGKEY_FUNNEL_EMITTER = "funnel(%d).emitter";

    public static final String CFGKEY_PROPERTY_TEMPLATE = "%s(%d).property";
    public static final String CFGATTR_CLASSNAME = "class";
    public static final String CFGATTR_ID = "id";
    public static final String CFGATTR_EMITTER_THREAD_COUNT = "emitter-threads";
    public static final String CFGATTR_AUTOSTART = "auto-start";
    public static final String CFGATTR_QSIZE = "qsize";
    public static final String CFGATTR_RATELIMIT = "rate-limit";
    public static final String CFGATTR_INIT_POSITIONS_FROM = "initPositionsFrom";
    public static final String CFGATTR_PROP_NAME = "name";
    public static final String CFGATTR_PROP_VALUE = "value";
    public static final String CFGATTR_PROP_REF = "ref";
    public static final String CFGATTR_PROP_TYPE = "type";
    public static final String CFGATTR_PROP_DEFAULT = "default";

    //
    public static final String DEFAULT_FUNNEL_CLASSNAME = "com.aol.advertising.qiao.agent.DataFunnel";
    public static final String DEFAULT_FUNNEL_EMITTERCONTAINER_CLASSNAME = "com.aol.advertising.qiao.emitter.DataSpray";
    public static final String DEFAULT_STAT_STORE_CLASSNAME = "com.aol.advertising.qiao.management.metrics.StatisticsStore";
    public static final String DEFAULT_STAT_CALCULATOR_CLASSNAME = "com.aol.advertising.qiao.management.metrics.StatsCalculator";
    public static final String DEFAULT_STAT_COLLECTOR_CLASSNAME = "com.aol.advertising.qiao.management.metrics.StatsCollector";
    public static final String DEFAULT_FUNNEL_DATAPIPE_CLASSNAME = "com.aol.advertising.qiao.agent.DataPipe";
    //
    //
    public static final String MBEAN_COMMON_MAP_ID = "commonMBeanMap";
    public static final String MBEAN_APPLICATION_MAP_ID = "applicationMBeanMap";

    public static final String NULL_EVENT_CLASS = "NULL_EVENT_CLASS_HOLDER";
    //
    public static final String QIAO = "QIAO ";

}
