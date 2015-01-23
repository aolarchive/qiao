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
 * File Name:   IStatsCalculatorAware.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management;

import com.aol.advertising.qiao.management.metrics.StatsCalculator;

public interface IStatsCalculatorAware
{
    public void setStatsCalculator(StatsCalculator statsCalculator);

}
