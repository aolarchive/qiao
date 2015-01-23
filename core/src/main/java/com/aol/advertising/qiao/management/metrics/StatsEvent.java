/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2011 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   StatsEvent.java	
 * Description:
 * @author:     ytung
 * @version:    1.0
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management.metrics;

import org.springframework.context.ApplicationEvent;

import com.aol.advertising.qiao.agent.IFunnel;
import com.aol.advertising.qiao.util.IntervalMetric;

/**
 * A StatsEvent object represents a statistics event. It can be published by
 * calling the publishEvent() method on an ApplicationEventPublisher.
 */
public class StatsEvent extends ApplicationEvent
{
    public static enum StatsOp
    {
        /**
         * Increment by 1
         */
        INCR,
        /**
         * Decrement by 1
         */
        DECR,
        /**
         * Increment by some value
         */
        INCRBY,
        /**
         * Decrement by some value
         */
        DECRBY,
        /**
         * Set to the specified value
         * 
         */
        SET,
        /**
         * Increment by the specified interval metric
         */
        INCRBY_INTVL_STAT,
        /**
         * Set to the interval metric
         */
        SET_INTVL_STAT,
        /**
         * Reset counter to 0
         */
        RESET
    };

    private static final long serialVersionUID = 5957753277801855497L;

    private StatsOp statOp;
    private String statKey;
    private long statValue;
    private String statSourceId;
    private String statSourceKey;
    private IFunnel funnel;
    private String statStoreId;
    //
    private IntervalMetric statIMetric;


    /**
     * @param source
     *            the component that published the event (never null)
     * @param statSourceId
     *            the publishing source identifier
     * @param statOp
     *            the operation to be performed by the listener
     * @param statKey
     *            the key
     * @param statValue
     *            the value
     */

    public StatsEvent(Object source, String statSourceId, String statStoreId,
            StatsOp statOp, String statKey, Long statValue)
    {
        super(source);
        this.statSourceId = statSourceId;
        this.statStoreId = statStoreId;
        this.statOp = statOp;
        this.statKey = statKey;
        this.statValue = statValue;
        this.statSourceKey = statSourceId + ":" + statKey;
    }


    public StatsEvent(Object source, String statSourceId, String statStoreId,
            StatsOp statOp, String statKey, IntervalMetric statValue)
    {
        super(source);
        this.statSourceId = statSourceId;
        this.statStoreId = statStoreId;
        this.statOp = statOp;
        this.statKey = statKey;
        this.statIMetric = statValue;
        this.statSourceKey = statSourceId + ":" + statKey;
    }


    /**
     * Get key
     * 
     * @return key
     */
    public String getStatKey()
    {
        return statKey;
    }


    /**
     * Set metric's key
     * 
     * @param statKey
     *            metric's key
     */
    public void setStatKey(String statKey)
    {
        this.statKey = statKey;
    }


    /**
     * Get metric's value
     * 
     * @return metric value
     */
    public long getStatValue()
    {
        return statValue;
    }


    /**
     * Set metric's value
     * 
     * @param statValue
     *            metrics value
     */
    public void setStatValue(long statValue)
    {
        this.statValue = statValue;
    }


    /**
     * Get the publishing source identifier
     * 
     * @return the publishing source identifier
     */
    public String getStatSourceId()
    {
        return statSourceId;
    }


    /**
     * Set the publishing source identifier
     * 
     * @param statSourceId
     *            the publishing source identifier
     */
    public void setStatSourceId(String statSourceId)
    {
        this.statSourceId = statSourceId;
    }


    /**
     * Get the operation
     * 
     * @return the operation
     */
    public StatsOp getStatOp()
    {
        return statOp;
    }


    /**
     * Set the operation
     * 
     * @param statOp
     *            the operation
     */
    public void setStatOp(StatsOp statOp)
    {
        this.statOp = statOp;
    }


    public String getStatSourceKey()
    {
        return statSourceKey;
    }


    public void setStatSourceKey(String statSourceKey)
    {
        this.statSourceKey = statSourceKey;
    }


    public IFunnel getFunnel()
    {
        return funnel;
    }


    public boolean fromFunnel()
    {
        return funnel != null;
    }


    public String getStatStoreId()
    {
        return statStoreId;
    }


    public IntervalMetric getStatIMetric()
    {
        return statIMetric;
    }


    public void setStatIMetric(IntervalMetric statIMetric)
    {
        this.statIMetric = statIMetric;
    }
}
