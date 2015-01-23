/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2014 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   IInjectBookKeeper.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector;

import com.aol.advertising.qiao.management.QiaoFileBookKeeper;

public interface IInjectBookKeeper
{
    public void setBookKeeper(QiaoFileBookKeeper bookKeeper);

}
