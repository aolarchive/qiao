package com.aol.advertising.qiao.agent;

import java.util.List;

import com.aol.advertising.qiao.management.ISuspendable;

public interface IContainer extends ISuspendable
{
    public void init() throws Exception;


    public void start() throws Exception;


    public void shutdown();


    public void setAgents(List<IAgent> agentList);

}
