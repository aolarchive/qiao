/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2011-2012 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   RateLimiter.java	
 * Description:
 * @author:     ytung
 * @version:    2.0.1
 *
 ****************************************************************************/

package com.aol.advertising.qiao.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * RateLimiter limits the maximum number of transactions can be performed in a
 * second. It maintains a set of permits. Before performing a rate-limited
 * action, a thread must acquire a permit from RateLimiter, guaranteeing that it
 * can proceed. By doing so, RateLimiter provides stable throughput and requests
 * are granted smoothly spreading over each second.
 * <p>
 * Usage example: <code>
 *    RateLimiter r = RateLimiter.create(100);  // rate: 100 events/sec
 *    ...
 *    while (active)
 *    {
 *       ...
 *       r.acquire();
 *       // dispatch an event
 *       ...
 *    }
 *    
 *    r.destroy();  // release resources
 * </code>
 */
public class RateLimiter implements Runnable
{
    private final static long TOT_NANOS = TimeUnit.SECONDS.toNanos(1);
    private final static Logger logger = Logger.getLogger(RateLimiter.class);
    private int maxPermitsPerSec; // maximum permits can be granted per seconds
    private long intervalNanos; // interval between granted permits
    private Semaphore availables; // maintains allowable permits
    private long startOfCurrentPeriod = 0L;
    private Lock lock = new ReentrantLock();
    private int acquiredPermits = 0; // permits have been granted in the current second

    /*
     * We could have used a multiple-threaded executor service and let it shared
     * among multiple instances of RateLimiter. However since RateLimiter is
     * meant for supporting high transaction rate, one thread per instance is
     * better for Qos.
     */
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor();
    private ScheduledFuture< ? > resetFuture;
    private int schedInitDelay = 10; // in msec
    private int schedInterval = 1000; // in msec


    /**
     * Gets a new instance of RateLimiter which supports the given rate.
     * 
     * @param requestsPerSec
     *            maximum requests can be granted per second
     * @return an instance of RateLimiter
     */
    public static RateLimiter create(int requestsPerSec)
    {
        RateLimiter r = new RateLimiter();
        r.maxPermitsPerSec = requestsPerSec;
        r.init();
        return r;
    }


    private void init()
    {
        intervalNanos = TOT_NANOS / maxPermitsPerSec;
        logger.debug("time period between permits (nanos): " + intervalNanos);

        availables = new Semaphore(maxPermitsPerSec);
        startOfCurrentPeriod = System.nanoTime();
        resetFuture = scheduler.scheduleAtFixedRate(this, schedInitDelay,
                schedInterval, TimeUnit.MILLISECONDS);
    }


    /**
     * Acquires a permit from this instance, blocking until the request can be
     * granted.
     * 
     * @throws InterruptedException
     */
    public void acquire() throws InterruptedException
    {
        lock.lock();
        try
        {
            //logger.info("avail: " + availables.availablePermits());

            availables.acquire();
            long target = getTargetTime(acquiredPermits++); // set to beginning of time period
            if (System.nanoTime() > target)
                return;

            TimeUnit.NANOSECONDS.sleep(target - System.nanoTime());
        }
        finally
        {
            lock.unlock();
        }
    }


    private void reset()
    {
        int used = acquiredPermits;
        acquiredPermits = 0;
        startOfCurrentPeriod = System.nanoTime();
        availables.release(used);
    }


    private long getTargetTime(int permitSequenceNo)
    {
        return startOfCurrentPeriod + permitSequenceNo * intervalNanos;

    }


    public void suspend()
    {
        resetFuture.cancel(false);
    }


    public void resume()
    {
        resetFuture = scheduler.scheduleAtFixedRate(this, schedInitDelay,
                schedInterval, TimeUnit.MILLISECONDS);
    }


    public void destroy()
    {
        CommonUtils.shutdownAndAwaitTermination(scheduler);
    }


    @Override
    public void run()
    {
        //logger.info("<tick>");
        reset();
    }


    public void modifyRate(int newRate)
    {
        suspend();
               
        maxPermitsPerSec = newRate;
        intervalNanos = TOT_NANOS / maxPermitsPerSec;
        logger.debug("time period between permits (nanos): " + intervalNanos);

        availables = new Semaphore(maxPermitsPerSec);
        startOfCurrentPeriod = System.nanoTime();
        
        resume();
    }
}
