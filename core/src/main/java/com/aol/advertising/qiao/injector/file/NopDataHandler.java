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
