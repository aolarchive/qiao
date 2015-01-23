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
 * File Name:   IMessageHandler.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/
 
package com.aol.advertising.qiao.emitter.handler;


public interface IMessageHandler<S, T>
{
    public T handle(S event);
}
