/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2014 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   KafkaEmitter.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.emitter.handler.IMessageHandler;
import com.aol.advertising.qiao.event.IKafkaEvent;
import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.exception.QiaoOperationException;
import com.aol.advertising.qiao.management.IStatsCalculatorAware;
import com.aol.advertising.qiao.management.IStatsCollectable;
import com.aol.advertising.qiao.management.metrics.IStatisticsStore;
import com.aol.advertising.qiao.management.metrics.PubStats;
import com.aol.advertising.qiao.management.metrics.PubStats.StatType;
import com.aol.advertising.qiao.management.metrics.StatsCalculator;
import com.aol.advertising.qiao.management.metrics.StatsEnum;
import com.aol.advertising.qiao.management.metrics.StatsEvent;
import com.aol.advertising.qiao.management.metrics.StatsEvent.StatsOp;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.IntervalMetric;
import com.aol.advertising.qiao.util.StatsUtils;

@ManagedResource
public class KafkaEmitter<K, V> extends AbstractDataEmitter implements
        IStatsCollectable, IStatsCalculatorAware
{

    public class KafkaProducer
    {
        Producer<K, V> producer;


        public KafkaProducer(Producer<K, V> producer)
        {
            this.producer = producer;
        }


        public void send(String topic, IKafkaEvent<K, V> event)
        {
            KeyedMessage<K, V> msg = new KeyedMessage<K, V>(topic,
                    event.getKey(), event.getMessage());
            long ts_start = System.nanoTime();
            producer.send(msg);
            long dur = System.nanoTime() - ts_start;
            stats.update(dur);

            numDispatched.incrementAndGet();
        }


        public void close()
        {
            producer.close();
        }
    }

    public static enum ACK_POLICY
    {
        NO_ACK("0"), REPLICA_RECEIVED("1"), ALL_RECEIVED("-1");

        private String value;


        ACK_POLICY(String v)
        {
            value = v;
        }


        public String value()
        {
            return value;
        }


        public String toString()
        {
            return this.name();
        }


        public static ACK_POLICY find(String ack)
        {
            return ACK_POLICY.valueOf(ack);
        }
    }

    public static final String PROP_BROKER_LIST = "metadata.broker.list";
    public static final String PROP_SERIALIZER_CLASS = "serializer.class";
    public static final String PROP_KEY_SERIALIZER_CLASS = "key.serializer.class";
    public static final String PROP_PARTITIONER_CLASS = "partitioner.class";
    public static final String PROP_REQUEST_REQUIRED_ACKS = "request.required.acks";
    public static final String PROP_PRODUCER_TYPE = "producer.type";
    public static final String PROP_MSG_MAX_RETRIES = "message.send.max.retries";
    public static final String PROP_CLIENT_ID = "client.id";
    public static final String PROP_BATCH_NUM_MSGS = "batch.num.messages";
    public static final String PROP_BATCH_BUFFERING_MAX_MS = "queue.buffering.max.ms";
    public static final String PROP_QUEUE_BUFFERING_MAX_MESSAGES = "queue.buffering.max.messages";
    public static final String PROP_SEND_BUFFER_BYTES = "send.buffer.bytes";
    //
    public static final String KAFKA_PROPERTY_FILE = "kafka.properties";
    //
    public static final ACK_POLICY DEFAULT_ACK_POLICY = ACK_POLICY.NO_ACK;
	private static final String DEFAULT_PARTITIONER_CLASSNAME = "com.aol.advertising.qiao.emitter.kafka.partitioner.RoundRobinPartitioner";

	
    private String brokerList; // separated by comma
    private String serializerClassname; // default = "kafka.serializer.DefaultEncoder"
    private String keySerializerClassname; // default to serializerClassname
    private String partitionerClassname; // default = "kafka.producer.DefaultPartitioner"
    private ACK_POLICY ackPolicy = DEFAULT_ACK_POLICY;
    private String acknowledgePolicy;
    private String producerType; // default = SYNC
    private String clientId = ""; // default to local host address
    private int msgSendMaxRetries = 3;
    private IMessageHandler<Object, IKafkaEvent<K, V>> messageHandler;
    private String topic;
    private int messagesPerBatch = 0; // default 200
    private int messagesBatchTimeoutMS = 0; // default 5000
    private int maxMessagesBuffering = 0; // default 10000
    private int sendBufferBytes = 0; // default 100K
    //
    private int numProducers = 1;
    private ThreadLocal<KafkaProducer> threadLocalProducers = new ThreadLocal<KafkaProducer>();
    private Deque<KafkaProducer> producersStack = new ConcurrentLinkedDeque<KafkaProducer>();
    private List<KafkaProducer> producersPool = new ArrayList<KafkaProducer>();

    private StatsCalculator statsCalculator;
    private String statDispatchedKey = "kafka_dispatched";
    private AtomicLong numDispatched = new AtomicLong(0);
    private String statKeyDispatchTime = "kafka_" + StatsEnum.EMIT_TIME.value()
            + "_nano";
    private IntervalMetric stats;


    @Override
    public void init() throws Exception
    {
        _validate();

        stats = new IntervalMetric(statKeyDispatchTime);

        Properties props = buildProperties();
        ProducerConfig config = new ProducerConfig(props);
        _createProducersPool(config);
        producersStack.addAll(producersPool);

        _registerStatsCollector();
        _registerStatsCalculator();
    }


    private void _createProducersPool(ProducerConfig config)
    {
        logger.info("creating " + numProducers + " producers...");
        for (int i = 0; i < numProducers; i++)
        {
            Producer<K, V> p = new Producer<K, V>(config);
            producersPool.add(new KafkaProducer(p));
        }
    }


    private Properties buildProperties()
    {
        Properties props = new Properties();
        props.put(PROP_BROKER_LIST, brokerList);
        props.put(PROP_REQUEST_REQUIRED_ACKS, ackPolicy.value);
        props.put(PROP_MSG_MAX_RETRIES, String.valueOf(msgSendMaxRetries));
        props.put(PROP_CLIENT_ID, clientId);
        props.put(PROP_REQUEST_REQUIRED_ACKS, ackPolicy.value);

        if (serializerClassname != null)
            props.put(PROP_SERIALIZER_CLASS, serializerClassname);
        if (keySerializerClassname != null)
            props.put(PROP_KEY_SERIALIZER_CLASS, keySerializerClassname);
        
        
        
        if (partitionerClassname != null)
            props.put(PROP_PARTITIONER_CLASS, partitionerClassname);
        else
        {
        	String val = (String) props.get(PROP_PARTITIONER_CLASS);
        	if (val == null || val.length() == 0) {
                props.put(PROP_PARTITIONER_CLASS, DEFAULT_PARTITIONER_CLASSNAME);
                logger.info("Kafka Producer uses non-sticky round-robin partitioner.");
        	}
        }
        if (producerType != null)
            props.put(PROP_PRODUCER_TYPE, producerType);
        if (messagesPerBatch > 0)
            props.put(PROP_BATCH_NUM_MSGS, String.valueOf(messagesPerBatch));
        if (messagesBatchTimeoutMS > 0)
            props.put(PROP_BATCH_BUFFERING_MAX_MS,
                    String.valueOf(messagesBatchTimeoutMS));
        if (maxMessagesBuffering > 0)
            props.put(PROP_QUEUE_BUFFERING_MAX_MESSAGES,
                    String.valueOf(maxMessagesBuffering));
        if (sendBufferBytes > 0)
            props.put(PROP_SEND_BUFFER_BYTES, String.valueOf(sendBufferBytes));

        String prop_path = CommonUtils.getQiaoConfigDir() + File.separator
                + KAFKA_PROPERTY_FILE;
        File prop_file = new File(prop_path);
        Properties props2 = loadProperties(prop_file);
        if (props2 != null)
            props.putAll(props2);

        return props;
    }


    private Properties loadProperties(File file)
    {
        if (!file.exists())
            return null;

        Properties props = new Properties();

        FileReader reader;
        try
        {
            reader = new FileReader(file);
            props.load(reader);
            return props;
        }
        catch (FileNotFoundException e)
        {
            logger.warn("File not found: " + file.getName());
        }
        catch (IOException e)
        {
            logger.warn("failed loading property file " + file.getName()
                    + " => " + e.getClass().getSimpleName() + ": "
                    + e.getMessage());
        }

        return null;
    }


    private void _validate() throws Exception
    {
        if (brokerList == null)
            throw new ConfigurationException("missing required brokerList");

        if (topic == null)
            throw new ConfigurationException("missing required topic");

        if (clientId == null || clientId.length() == 0)
            clientId = InetAddress.getLocalHost().getHostAddress();

        if (acknowledgePolicy != null)
        {
            try
            {
                ackPolicy = ACK_POLICY.find(acknowledgePolicy);
            }
            catch (IllegalArgumentException e)
            {
                logger.warn("invalid acknowledgePolicy value: "
                        + acknowledgePolicy + ".  Use default setting "
                        + DEFAULT_ACK_POLICY);

                ackPolicy = DEFAULT_ACK_POLICY;
            }
            catch (Exception e)
            {
                logger.warn("Failed to validate acknowledgePolicy value: "
                        + acknowledgePolicy + " => "
                        + e.getClass().getSimpleName() + ": " + e.getMessage()
                        + ".  Use default setting " + DEFAULT_ACK_POLICY);

                ackPolicy = DEFAULT_ACK_POLICY;
            }
        }
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


    @Override
    public void removeThreadLocal()
    {
        KafkaProducer producer = threadLocalProducers.get();
        if (producer != null)
        {
            producer.close();
            threadLocalProducers.remove();
        }
    }


    @Override
    public void process(Object data)
    {
        KafkaProducer producer = getThreadLocalProducer();
        if (messageHandler != null)
        {
            IKafkaEvent<K, V> event = messageHandler.handle(data);
            if (event != null)
                producer.send(topic, event);
        }
        else if (data instanceof IKafkaEvent)
        {
            @SuppressWarnings("unchecked")
            IKafkaEvent<K, V> event = (IKafkaEvent<K, V>) data;
            producer.send(topic, event);
        }
        else
        {
            throw new QiaoOperationException("Unable to process data => "
                    + data.toString());
        }

    }


    private KafkaProducer getThreadLocalProducer()
            throws QiaoOperationException
    {
        KafkaProducer producer = threadLocalProducers.get();
        if (producer == null)
        {
            producer = producersStack.poll();
            if (producer == null)
                throw new QiaoOperationException("No producer available");

            threadLocalProducers.set(producer);
        }

        return producer;
    }


    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            threadLocalProducers = new ThreadLocal<KafkaProducer>();
        }
    }


    @Override
    public void resume()
    {
        if (isSuspended.compareAndSet(true, false))
        {
            producersStack.addAll(producersPool);
        }
    }


    @Override
    public void shutdown()
    {
        running = false;
        for (KafkaProducer p : producersPool)
        {
            p.close();
        }
    }


    @Override
    public void setStatsCalculator(StatsCalculator statsCalculator)
    {
        this.statsCalculator = statsCalculator;
    }


    @ManagedAttribute
    public String getBrokerList()
    {
        return brokerList;
    }


    public void setBrokerList(String brokerList)
    {
        this.brokerList = brokerList;
    }


    @ManagedAttribute
    public String getSerializerClassname()
    {
        return serializerClassname;
    }


    public void setSerializerClassname(String serializerClassname)
    {
        this.serializerClassname = serializerClassname;
    }


    @ManagedAttribute
    public String getPartitionerClassname()
    {
        return partitionerClassname;
    }


    public void setPartitionerClassname(String partitionerClassname)
    {
        this.partitionerClassname = partitionerClassname;
    }


    @ManagedAttribute
    public String getProducerType()
    {
        return producerType;
    }


    public void setProducerType(String producerType)
    {
        this.producerType = producerType;
    }


    @ManagedAttribute
    public String getClientId()
    {
        return clientId;
    }


    public void setClientId(String clientId)
    {
        this.clientId = clientId;
    }


    @ManagedAttribute
    public int getMsgSendMaxRetries()
    {
        return msgSendMaxRetries;
    }


    public void setMsgSendMaxRetries(int msgSendMaxRetries)
    {
        this.msgSendMaxRetries = msgSendMaxRetries;
    }


    @ManagedAttribute
    public String getTopic()
    {
        return topic;
    }


    public void setTopic(String topic)
    {
        this.topic = topic;
    }


    @ManagedAttribute
    public int getNumProducers()
    {
        return numProducers;
    }


    public void setNumProducers(int numProducers)
    {
        this.numProducers = numProducers;
    }


    public void setMessageHandler(
            IMessageHandler<Object, IKafkaEvent<K, V>> messageHandler)
    {
        this.messageHandler = messageHandler;
    }


    public void setStatDispatchedKey(String statDispatchedKey)
    {
        this.statDispatchedKey = statDispatchedKey;
    }


    public void setStatKeyDispatchTime(String statKeyDispatchTime)
    {
        this.statKeyDispatchTime = statKeyDispatchTime;
    }


    @ManagedAttribute
    public String getAcknowledgePolicy()
    {
        return acknowledgePolicy;
    }


    public void setAcknowledgePolicy(String acknowledgePolicy)
    {
        this.acknowledgePolicy = acknowledgePolicy;
    }


    @ManagedAttribute
    public String getKeySerializerClassname()
    {
        return keySerializerClassname;
    }


    public void setKeySerializerClassname(String keySerializerClassname)
    {
        this.keySerializerClassname = keySerializerClassname;
    }


    @ManagedAttribute
    public int getMessagesPerBatch()
    {
        return messagesPerBatch;
    }


    public void setMessagesPerBatch(int messagesPerBatch)
    {
        this.messagesPerBatch = messagesPerBatch;
    }


    @ManagedAttribute
    public int getMessagesBatchTimeoutMS()
    {
        return messagesBatchTimeoutMS;
    }


    public void setMessagesBatchTimeoutMS(int messagesBatchTimeoutMS)
    {
        this.messagesBatchTimeoutMS = messagesBatchTimeoutMS;
    }


    @ManagedAttribute
    public int getMaxMessagesBuffering()
    {
        return maxMessagesBuffering;
    }


    public void setMaxMessagesBuffering(int maxMessagesBuffering)
    {
        this.maxMessagesBuffering = maxMessagesBuffering;
    }


    @ManagedAttribute
    public int getSendBufferBytes()
    {
        return sendBufferBytes;
    }


    public void setSendBufferBytes(int sendBufferBytes)
    {
        this.sendBufferBytes = sendBufferBytes;
    }


    @Override
    public void setEmitterThreadCount(int threadCount)
    {
        this.numProducers = threadCount;
    }

}
