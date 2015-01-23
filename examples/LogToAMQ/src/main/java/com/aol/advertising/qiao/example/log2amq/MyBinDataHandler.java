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
 * File Name:   MyBinDataHandler.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.example.log2amq;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.injector.file.IReader;
import com.aol.advertising.qiao.injector.file.ITailerDataHandler;
import com.aol.advertising.qiao.management.FileReadingPositionCache;

public class MyBinDataHandler implements ITailerDataHandler<ByteBuffer>
{
    Logger logger = LoggerFactory.getLogger(this.getClass());
    volatile List<String> result;
    int capacity = 2048;
    ByteBuffer buffer;
    StringBuffer sb = new StringBuffer();
    int positionAdjustment = 0;


    @Override
    public Iterator< ? > onData(ByteBuffer inData)
    {
        inData.flip();

        ByteBuffer data;
        if (!isEmpty(buffer))
        {
            buffer.flip();

            data = ByteBuffer.allocate(buffer.limit() + inData.limit());
            data.put(buffer);
            data.put(inData);
            buffer.clear();

            data.flip();
        }
        else
        {
            data = inData.asReadOnlyBuffer();
        }

        result = new ArrayList<String>();

        while (data.hasRemaining())
        {
            byte ch = data.get();
            if (ch != '\n')
                continue;

            ByteBuffer t = data.slice();
            byte[] dst = new byte[data.position() - 1];
            data.rewind();
            data.get(dst);

            String line = new String(dst);
            result.add("|" + line + "|");
            data = t;

        }

        if (data.limit() > 0) // remaining data
        {
            //System.out.println("Saving remaining data: len=" + data.limit());
            data.flip();
            buffer.rewind();
            buffer.limit(data.limit());
            buffer.put(data);
            positionAdjustment = -buffer.limit();
        }

        return result.iterator();
     }


    private boolean isEmpty(ByteBuffer bbuf)
    {
        return (bbuf.position() == 0 && bbuf.limit() == 0);
    }


    @Override
    public FileReadingPositionCache.FileReadState init(IReader<ByteBuffer> tailer) throws Exception
    {
        logger.info("init>");
        buffer = ByteBuffer.allocate(capacity);
        buffer.flip(); // pos=0, limit=0

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


    public void setCapacity(int capacity)
    {
        this.capacity = capacity;
    }


    @Override
    public int getPositionAdjustment()
    {
        return positionAdjustment;
    }

}
