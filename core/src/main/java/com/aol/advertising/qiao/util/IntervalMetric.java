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

package com.aol.advertising.qiao.util;

public class IntervalMetric
{
    private String name;
    private long count;
    private long time; // in milliseconds


    public IntervalMetric(String name)
    {
        this.name = name;
    }


    public IntervalMetric(String name, long count, long time)
    {
        this.name = name;
        this.count = count;
        this.time = time;
    }


    public synchronized void update(long ntime)
    {
        time += ntime;
        count++;
    }


    public synchronized void incr(IntervalMetric delta)
    {
        time += delta.time;
        count += delta.count;
    }


    public synchronized double avg()
    {
        if (count == 0)
            return 0;

        return 1.0 * time / count;
    }


    public synchronized double avgAndReset()
    {
        try
        {
            if (count == 0)
                return 0;

            return 1.0 * time / count;
        }
        finally
        {
            count = 0;
            time = 0;
        }
    }


    public synchronized void reset()
    {
        count = 0;
        time = 0;
    }


    public synchronized IntervalMetric getAndReset()
    {
        try
        {
            return this.duplicate();
        }
        finally
        {
            count = 0;
            time = 0;
        }
    }


    public String getName()
    {
        return name;
    }


    public long getCount()
    {
        return count;
    }


    public IntervalMetric diff(IntervalMetric other)
    {
        if (other == null)
            return this.duplicate();

        IntervalMetric res = new IntervalMetric(this.name, this.count
                - other.count, this.time - other.time);
        return res;
    }


    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof IntervalMetric))
            return false;

        IntervalMetric other = (IntervalMetric) obj;
        if (!this.name.equals(other.name))
            return false;

        if (this.count != other.count || this.time != other.time)
            return false;

        return true;

    }


    @Override
    public String toString()
    {
        return String.format("[name=%s,count=%d,time=%d]", name, count, time);
    }


    public long getTime()
    {
        return time;
    }


    public IntervalMetric duplicate()
    {
        return new IntervalMetric(this.name, this.count, this.time);
    }
}
