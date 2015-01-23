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
 * File Name:   NopDataHandler.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector.file;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.management.FileReadingPositionCache;

public class NopDataHandler implements ITailerDataHandler<Object>
{
    Logger logger = LoggerFactory.getLogger(this.getClass());
    volatile List<Object> result;


    @Override
    public Iterator<Object> onData(Object data)
    {
        result = new ArrayList<Object>();
        result.add(data);
        return result.iterator();       
    }


    @Override
    public FileReadingPositionCache.FileReadState init(IReader<Object> tailer) throws Exception
    {
         return null;
    }


    @Override
    public void close()
    {
        logger.info("close>");
    }


    @Override
    public void onException(Throwable ex)
    {
        logger.error("Exception> " + ex.getMessage(), ex);

    }


    @Override
    public void fileNotFound()
    {
        logger.warn("fileNotFound>");

    }


    @Override
    public void fileRotated()
    {
        logger.info("fileRoated>");
    }


    @Override
    public int getPositionAdjustment()
    {
        // TODO Auto-generated method stub
        return 0;
    }

}
