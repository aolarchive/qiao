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
import java.util.concurrent.atomic.AtomicLong;

import com.aol.advertising.qiao.event.EventWrapper;
import com.aol.advertising.qiao.util.CommonUtils;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

public class DataRingBuffer implements IDataPipe
{
    private int capacity = 1;
    private AtomicLong numWrites = new AtomicLong(0);
    private AtomicLong numReads = new AtomicLong(0);

    private RingBuffer<EventWrapper> ring;
    private Sequence sequence;
    private SequenceBarrier consumerBarrier;
    private WaitStrategy waitStrategy;


    @Override
    public void start()
    {
    }


    public void init() throws IOException
    {

        if (!CommonUtils.isPowerOfTwo(capacity))
            capacity = CommonUtils.power2(capacity);

        if (waitStrategy == null)
            waitStrategy = new YieldingWaitStrategy();

        ring = RingBuffer.createSingleProducer(EventWrapper.EVENT_FACTORY,
                capacity, waitStrategy);

        sequence = new Sequence(0); //start with 0 instead of default -1
        ring.addGatingSequences(sequence);

        consumerBarrier = ring.newBarrier();

    }


    public void write(Object data)
    {
        long seq = ring.next();
        try
        {
            EventWrapper event = ring.get(seq);
            event.setEvent(data);
        }
        finally
        {
            ring.publish(seq);
        }

        numWrites.incrementAndGet();
    }


    public Object read() throws InterruptedException, AlertException,
            TimeoutException
    {
        long next_seq = sequence.incrementAndGet();

        long avail_seq = consumerBarrier.waitFor(next_seq);

        if (next_seq <= avail_seq)
        {
            EventWrapper w = ring.get(next_seq);

            numReads.incrementAndGet();
            return w.getEvent();
        }
        else
            throw new IllegalStateException(
                    "available sequence number < requested sequence number => "
                            + avail_seq + "<" + next_seq);
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
        return capacity - (int) ring.remainingCapacity();
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


    public void setWaitStrategy(WaitStrategy waitStrategy)
    {
        this.waitStrategy = waitStrategy;
    }

}
