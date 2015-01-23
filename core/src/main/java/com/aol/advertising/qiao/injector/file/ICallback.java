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
 * File Name:   ICallback.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/
 
package com.aol.advertising.qiao.injector.file;

import com.aol.advertising.qiao.agent.IDataPipe;


public interface ICallback
{
    public void receive(Object data);


    public void setDataPipe(IDataPipe pipe);
}