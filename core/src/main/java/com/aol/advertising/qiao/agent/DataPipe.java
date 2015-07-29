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

package com.aol.advertising.qiao.agent;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class DataPipe implements IDataPipe
{
    private BlockingQueue<Object> queue;
    private int capacity = 1;
    private AtomicLong numWrites = new AtomicLong(0);
    private AtomicLong numReads = new AtomicLong(0);


    public void init() throws IOException
    {
        queue = new ArrayBlockingQueue<Object>(capacity, true);
    }


    @Override
    public void start() throws Exception
    {
    }


    public void write(Object data)
    {
        try
        {
            queue.put(data);
            numWrites.incrementAndGet();
        }
        catch (InterruptedException e)
        {
        }
    }


    public Object read() throws InterruptedException
    {
        Object o = queue.take();
        numReads.incrementAndGet();
        return o;
    }


    public int getCapacity()
    {
        return capacity;
    }


    public void setCapacity(int capacity)
    {
        this.capacity = capacity;
    }


    public int size()
    {
        return queue.size();
    }


    public boolean isEmpty()
    {
        return size() == 0;
    }


    public long getNumWrites()
    {
        return numWrites.get();
    }


    public long getNumReads()
    {
        return numReads.get();
    }


    public long getAndResetNumWrites()
    {
        return numWrites.getAndSet(0);
    }


    public long getAndResetNumReads()
    {
        return numReads.getAndSet(0);
    }

}
