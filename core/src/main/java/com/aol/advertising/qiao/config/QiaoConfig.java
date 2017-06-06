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

package com.aol.advertising.qiao.config;

import java.util.HashMap;
import java.util.Map;

public class QiaoConfig
{
    protected MultiSubnodeConfiguration agentNodeConfig;
    protected Map<String, AgentConfig> agentConfigMap;


    public MultiSubnodeConfiguration getAgentNodeConfig()
    {
        return agentNodeConfig;
    }


    public void setAgentNodeConfig(MultiSubnodeConfiguration agentNodeConfig)
    {
        this.agentNodeConfig = agentNodeConfig;
    }


    public AgentConfig getAgentConfig(String id)
    {
        return agentConfigMap.get(id);
    }


    public Map<String, AgentConfig> getAgentConfigMap()
    {
        return agentConfigMap;
    }


    public void setAgentConfigMap(Map<String, AgentConfig> agentConfigMap)
    {
        this.agentConfigMap = agentConfigMap;
    }


    public void addAgentConfig(String agentId, AgentConfig agentConfig)
    {
        if (null == agentConfigMap)
            agentConfigMap = new HashMap<>();

        this.agentConfigMap.put(agentId, agentConfig);
    }


    public int getAgentCount()
    {
        return agentConfigMap.size();
    }

    // -----------------------------
    public class AgentConfig
    {
        protected MultiSubnodeConfiguration funnelConfig;
        protected Map<String, String> funnelClassNames; // <id, class>
        protected Map<String, FunnelComponents> funnelComponents;


        public MultiSubnodeConfiguration getFunnelConfig()
        {
            return funnelConfig;
        }


        public void setFunnelConfig(MultiSubnodeConfiguration funnelConfig)
        {
            this.funnelConfig = funnelConfig;
        }


        public Map<String, String> getFunnelClassNames()
        {
            return funnelClassNames;
        }


        public void setFunnelClassNames(Map<String, String> funnelClassNames)
        {
            this.funnelClassNames = funnelClassNames;
        }


        public Map<String, FunnelComponents> getFunnelComponents()
        {
            return funnelComponents;
        }


        public void setFunnelComponents(
                Map<String, FunnelComponents> funnelComponents)
        {
            this.funnelComponents = funnelComponents;
        }

    }

    public class FunnelComponents
    {
        private String id;
        private InjectorConfig sourceConfig;
        private EmitterConfig sinkConfig;


        public String getId()
        {
            return id;
        }


        public void setId(String id)
        {
            this.id = id;
        }


        public InjectorConfig getSourceConfig()
        {
            return sourceConfig;
        }


        public void setSourceConfig(InjectorConfig sourceConfig)
        {
            this.sourceConfig = sourceConfig;
        }


        public EmitterConfig getSinkConfig()
        {
            return sinkConfig;
        }


        public void setSinkConfig(EmitterConfig sinkConfig)
        {
            this.sinkConfig = sinkConfig;
        }
    }

}
