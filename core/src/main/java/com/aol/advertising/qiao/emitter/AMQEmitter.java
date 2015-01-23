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
 * File Name:   AMQEmitter.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.emitter.handler.DefaultAMQMessageCreator;
import com.aol.advertising.qiao.emitter.handler.IJmsMessageCreator;
import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.exception.QiaoOperationException;
import com.aol.advertising.qiao.management.ConnectionController;
import com.aol.advertising.qiao.management.ConnectionController.ConnectionType;
import com.aol.advertising.qiao.management.IConnectionControlListener;
import com.aol.advertising.qiao.management.IStatsCalculatorAware;
import com.aol.advertising.qiao.management.IStatsCollectable;
import com.aol.advertising.qiao.management.metrics.IStatisticsStore;
import com.aol.advertising.qiao.management.metrics.PubStats;
import com.aol.advertising.qiao.management.metrics.PubStats.StatType;
import com.aol.advertising.qiao.management.metrics.StatsCalculator;
import com.aol.advertising.qiao.management.metrics.StatsEnum;
import com.aol.advertising.qiao.management.metrics.StatsEvent;
import com.aol.advertising.qiao.management.metrics.StatsEvent.StatsOp;
import com.aol.advertising.qiao.util.IntervalMetric;
import com.aol.advertising.qiao.util.StatsUtils;

/**
 * AMQEmitter sends data to ActiveMQ queue or topic. A message factory class can
 * be set via the msgCreator property to create a JMS Message object for each
 * data item. If none is specified, DefaultAMQMessageCreator is used.
 */
@ManagedResource
public class AMQEmitter extends AbstractDataEmitter implements
        IStatsCollectable, IConnectionControlListener, IStatsCalculatorAware
{
    class Producer
    {
        Session session;
        MessageProducer producer;


        Producer(Session session, MessageProducer producer)
        {
            this.session = session;
            this.producer = producer;
        }


        public void send(Message msg) throws JMSException
        {
            producer.send(msg);
        }


        public void close()
        {
            try
            {
                producer.close();
                session.close();
            }
            catch (JMSException e)
            {
            }
        }
    }

    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    private IJmsMessageCreator<Object> msgCreator;
    private ActiveMQConnectionFactory connectionFactory;
    private String destinationName;
    private Destination destination;
    private boolean isQueue = true;
    private int numProducer = 1; // one producer per session per connection 
    private boolean transacted = false;
    private int ackMode = Session.AUTO_ACKNOWLEDGE;
    private int deliveryMode;
    private String brokerURL;

    private List<Connection> connList;

    //
    private ConnectionController connectionController;

    private AtomicBoolean isInitialized = new AtomicBoolean(false);

    private ThreadLocal<Producer> localProducer = new ThreadLocal<Producer>();

    private Deque<Producer> producersStack = new ConcurrentLinkedDeque<Producer>();
    private List<Producer> producersPool = new ArrayList<Producer>();

    private String statDispatchedKey = "amq_dispatched";
    private AtomicLong numDispatched = new AtomicLong(0);

    private String statKeyDispatchTime = "amq_" + StatsEnum.EMIT_TIME.value()
            + "_nano";
    private IntervalMetric stats;
    private StatsCalculator statsCalculator;


    @Override
    public void init() throws Exception
    {
        if (isInitialized.compareAndSet(false, true))
        {
            _validate();

            stats = new IntervalMetric(statKeyDispatchTime);

            _registerStatsCollector();
            _registerStatsCalculator();

            logger.info("brokerURL=" + brokerURL);
            if (brokerURL != null)
                connectionFactory.setBrokerURL(brokerURL);

            connList = new ArrayList<Connection>(numProducer);

            createJmsResources();

            if (connectionController != null)
            {
                connectionController.register(this, ConnectionType.OUTBOUND);
            }
        }
    }


    private void _validate()
    {
        if (destinationName == null || destinationName.trim().length() == 0)
            throw new ConfigurationException("Missing property destinationName");

        if (msgCreator == null)
            msgCreator = new DefaultAMQMessageCreator();
    }


    private void _registerStatsCollector()
    {

        final String clzname = this.getClass().getSimpleName();
        if (statsCollector != null)
            statsCollector.register(new Callable<Void>()
            {
                @Override
                public Void call()
                {
                    if (numDispatched.get() > 0)
                    {
                        eventPublisher.publishEvent(new StatsEvent(this,
                                clzname, funnelId, StatsOp.INCRBY,
                                statDispatchedKey, numDispatched.getAndSet(0)));

                    }

                    eventPublisher.publishEvent(new StatsEvent(this, clzname,
                            funnelId, StatsOp.INCRBY_INTVL_STAT,
                            statKeyDispatchTime, stats.getAndReset()));

                    return null;
                }
            });

    }


    private void _registerStatsCalculator()
    {
        if (statsCalculator != null)
        {
            IStatisticsStore statsStore = StatsUtils.getStatsStore(funnelId);
            if (statsStore == null)
                throw new ConfigurationException(funnelId
                        + " statistics store does not exist");

            Map<String, PubStats> counter_keys = new LinkedHashMap<String, PubStats>();

            PubStats pstats = new PubStats(StatType.INTERVAL_METRIC,
                    statKeyDispatchTime, false, true, false, false);
            counter_keys.put(pstats.getMetric(), pstats);

            statsCalculator.register(statsCalculator.new CalcCallable(
                    statsStore, counter_keys));
        }

    }


    /**
     * Create JMS connectins, sessions, producers, destination objects. (one
     * producer per connection)
     * 
     * @throws Exception
     */
    protected void createJmsResources() throws Exception
    {

        Connection conn;
        Session session;
        MessageProducer producer;

        // create connections
        logger.info("creating producers...");
        for (int i = 0; i < numProducer; i++)
        {
            conn = connectionFactory.createConnection();
            connList.add(conn);

            // create sessions for a connection
            session = conn.createSession(transacted, ackMode);

            if (destination == null)
            {
                if (isQueue)
                {
                    destination = session.createQueue(destinationName);
                }
                else
                {
                    destination = session.createTopic(destinationName);
                }
            }

            producer = session.createProducer(destination);
            producer.setDeliveryMode(deliveryMode);

            Producer p = new Producer(session, producer);
            producersPool.add(p);
            producersStack.add(p);
        }

        logger.info("AMQ producer(s) created, count=" + producersStack.size());
    }


    private Producer getProducer() throws QiaoOperationException
    {
        Producer producer = localProducer.get();
        if (producer == null)
        {
            producer = producersStack.poll();
            if (producer == null)
                throw new QiaoOperationException("No producer available");

            localProducer.set(producer);
        }

        return producer;
    }


    @Override
    public void process(Object data)
    {
        try
        {
            Producer producer = getProducer();
            Message msg = msgCreator.createMessage(producer.session, data);
            if (msg != null)
            {
                if (logger.isDebugEnabled())
                    logger.debug("send> " + data.toString());

                long ts_start = System.nanoTime();
                producer.send(msg);
                long dur = System.nanoTime() - ts_start;
                stats.update(dur);

                numDispatched.incrementAndGet();
            }
        }
        catch (JMSException e)
        {
            logger.warn(e.getClass().getName() + ": " + e.getMessage());
            running = false;
        }
        catch (QiaoOperationException e)
        {
            logger.error(e.getMessage(), e);
            running = false;
        }
        catch (Throwable t)
        {
            logger.error(t.getMessage(), t);
            running = false;
        }

    }


    @Override
    public void shutdown()
    {
        running = false;
        for (Producer p : producersPool)
        {
            p.close();
        }
    }


    @Override
    public void removeThreadLocal()
    {
        Producer producer = localProducer.get();
        if (producer != null)
        {
            producer.close();
            localProducer.remove();
        }
    }


    @ManagedOperation
    public void resetConnections()
    {
        producersStack.clear();

        for (Connection conn : connList)
        {
            try
            {
                conn.close();
            }
            catch (Exception e)
            {
                logger.error(e.getMessage());
            }
        }

        connList.clear();

        try
        {
            createJmsResources();
            this.start();
            logger.info(this.getClass().getSimpleName() + " re-started");
        }
        catch (Exception e)
        {
            logger.error("failed to create JMS resources - "
                    + e.getClass().getName() + ": " + e.getMessage(), e);
        }

    }


    @Override
    public void reset()
    {
        resetConnections();
    }


    /**
     * Required to properly create the message from an event.
     * 
     * @param msgCreator
     */

    public void setMsgCreator(IJmsMessageCreator<Object> msgCreator)
    {
        this.msgCreator = msgCreator;
    }


    public void setConnectionFactory(ActiveMQConnectionFactory connectionFactory)
    {
        this.connectionFactory = connectionFactory;
    }


    public void setDestinationName(String destinationName)
    {
        this.destinationName = destinationName;
    }


    // public void setNumConnections(int numConnections)
    // {
    //     this.numConnections = numConnections;
    // }

    public void setTransacted(boolean transacted)
    {
        this.transacted = transacted;
    }


    public void setAckMode(int ackMode)
    {
        this.ackMode = ackMode;
    }


    public void setDeliveryMode(int deliveryMode)
    {
        this.deliveryMode = deliveryMode;
    }


    public void setBrokerURL(String brokerURL)
    {
        this.brokerURL = brokerURL;
    }


    public void setStatDispatchedKey(String statDispatchedKey)
    {
        this.statDispatchedKey = statDispatchedKey;
    }


    public void setConnectionController(
            ConnectionController connectionController)
    {
        this.connectionController = connectionController;
    }


    @ManagedAttribute
    public int getNumProducers()
    {
        return numProducer;
    }


    @ManagedAttribute
    public String getDestinationName()
    {
        return destinationName;
    }


    @ManagedAttribute
    public String getBrokerURL()
    {
        return brokerURL;
    }


    @ManagedAttribute
    public boolean isQueue()
    {
        return isQueue;
    }


    @ManagedAttribute
    public boolean isTransacted()
    {
        return transacted;
    }


    @ManagedAttribute
    public int getAckMode()
    {
        return ackMode;
    }


    @ManagedAttribute
    public int getDeliveryMode()
    {
        return deliveryMode;
    }


    @ManagedAttribute
    @Override
    public boolean isRunning()
    {
        return running && !isSuspended();
    }


    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            localProducer = new ThreadLocal<Producer>();
        }
    }


    @Override
    public void resume()
    {
        if (isSuspended.compareAndSet(true, false))
        {
            for (Producer p : producersPool)
            {
                producersStack.add(p);
            }
        }
    }


    @Override
    public boolean isSuspended()
    {
        return isSuspended.get();
    }


    public void setStatKeyDispatchTime(String statKeyDispatchTime)
    {
        this.statKeyDispatchTime = statKeyDispatchTime;
    }


    @Override
    public void setStatsCalculator(StatsCalculator statsCalculator)
    {
        this.statsCalculator = statsCalculator;
    }


    @Override
    public void setEmitterThreadCount(int threadCount)
    {
        this.numProducer = threadCount;
    }

}
