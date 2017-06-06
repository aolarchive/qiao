package com.aol.advertising.qiao.agent;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedOperation;

import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.management.metrics.StatsManager;

public class QiaoContainer implements IContainer
{
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private List<IAgent> agentList;
    private StatsManager statsManager;
    private AtomicBoolean isSuspended = new AtomicBoolean(false);


    @Override
    public void init() throws Exception
    {
        statsManager.init();

        if (null == agentList)
        {
            throw new ConfigurationException("No agent defined");
        }

        for (IAgent agent : agentList)
        {
            agent.init();
        }

        logger.info(this.getClass().getName() + " initialized");

    }


    @Override
    public void start() throws Exception
    {
        for (IAgent agent : agentList)
        {
            agent.start();
        }

        statsManager.start();

        logger.info(this.getClass().getName() + " started");

        logger.info("\n   ------------\n   QIAO started\n   ------------");

    }


    @Override
    public void shutdown()
    {

        for (IAgent agent : agentList)
        {
            agent.shutdown();
        }

        statsManager.shutdown();

        logger.info(this.getClass().getName() + " shutdown");

    }


    @Override
    public void setAgents(List<IAgent> agentList)
    {
        this.agentList = agentList;
    }


    public List<IAgent> getAgentList()
    {
        return agentList;
    }


    public void setAgentList(List<IAgent> agentList)
    {
        this.agentList = agentList;
    }


    public void setStatsManager(StatsManager statsManager)
    {
        this.statsManager = statsManager;
    }


    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {

            for (IAgent agent : agentList)
            {
                agent.suspend();
            }

            statsManager.suspend();
        }
    }


    @Override
    public void resume()
    {

        if (isSuspended.compareAndSet(true, false))
        {

            for (IAgent agent : agentList)
            {
                agent.resume();
            }

            statsManager.resume();
        }
    }


    @Override
    public boolean isSuspended()
    {
        return isSuspended.get();
    }


    @ManagedOperation(description = "Reset statistics counters")
    public void resetStatsCounters()
    {
        logger.info("reset counters...");
        statsManager.resetCounters();
    }
}
