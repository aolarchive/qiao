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
 * File Name:   IBootstrap.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/
 
package com.aol.advertising.qiao.bootstrap;


public interface IBootstrap
{
    public void init() throws Exception;
    
    public void start() throws Exception;

    public void stop();
}
