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
 * File Name:   IDataPipe.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.agent;

import java.io.IOException;

public interface IDataPipe
{
    public void init() throws IOException;


    public void start() throws Exception;


    public void write(Object data);


    public Object read() throws Exception;


    public int getCapacity();


    public void setCapacity(int capacity);


    public long getNumWrites();


    public long getNumReads();


    public long getAndResetNumWrites();


    public long getAndResetNumReads();


    public boolean isEmpty();


    public int size();
}
