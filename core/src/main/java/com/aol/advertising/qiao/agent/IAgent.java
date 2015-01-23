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
 * File Name:   IAgent.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.agent;

import java.util.List;

import com.aol.advertising.qiao.management.ISuspendable;
import com.aol.advertising.qiao.management.QiaoFileBookKeeper;
import com.aol.advertising.qiao.util.cache.PositionCache;

public interface IAgent extends ISuspendable
{
    public void init() throws Exception;


    public void start() throws Exception;


    public void shutdown();


    public void setFunnels(List<IFunnel> funnelList);


    public void setBookKeeper(QiaoFileBookKeeper bookKeeper);


    //public void setPositionCache(PositionCache positionCache);

}
