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
 * File Name:   IntervalMetric.java	
 * Description:
 * @author:     ytung05
 *
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
