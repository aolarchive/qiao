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
 * File Name:   IFileEventListener.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/
 
package com.aol.advertising.qiao.injector.file.watcher;

import java.nio.file.Path;


public interface IFileEventListener
{
    public void init() throws Exception;
    
    public void onCreate(Path file);
    public void onDelete(Path file);
    public void onModify(Path file);
}
