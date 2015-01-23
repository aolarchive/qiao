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
 * File Name:   ITailer.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector.file;

import java.io.File;

import com.aol.advertising.qiao.management.FileReadingPositionCache;

/**
 * Interface for reader.
 * 
 * @param <T>
 */
public interface IReader<T>
{

    public void init() throws Exception;


    public void start() throws Exception;


    public void stop();


    public long getReadPosition();


    public File getFile();


    public FileReadingPositionCache.FileReadState getFileReadState();


    public void setFileReadState(
            FileReadingPositionCache.FileReadState readState);

}
