/****************************************************************************
 * Copyright (c) 2015 AOL Inc.
 * @author:     ytung05
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ****************************************************************************/

package com.aol.advertising.qiao;

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
            //if (buffer.limit() == 0)
            buffer.limit(data.limit());
            buffer.put(data);
            positionAdjustment = -buffer.limit();
        }
        else
            positionAdjustment = 0;

        System.out.format("positionAdjustment=%d\n", positionAdjustment);

        // int last_pos = inData.limit() - buffer.limit();
        return result.iterator();
    }


    private boolean isEmpty(ByteBuffer bbuf)
    {
        return (bbuf.position() == 0 && bbuf.limit() == 0);
    }


    @Override
    public FileReadingPositionCache.FileReadState init(
            IReader<ByteBuffer> tailer) throws Exception
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
