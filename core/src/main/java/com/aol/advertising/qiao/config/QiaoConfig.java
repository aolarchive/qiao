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

import java.util.Map;

public class QiaoConfig
{

    protected Map<String, String> agentClassNames;
    protected MultiSubnodeConfiguration agentsConfig;
    protected Map<String, Agent> agentsComponents;

    public Map<String, Agent> getAgentsComponents() {
        return agentsComponents;
    }

    public void setAgentsComponents(final Map<String, Agent> agentsComponents) {
        this.agentsComponents = agentsComponents;
    }

    public MultiSubnodeConfiguration getAgentsConfig() {
        return agentsConfig;
    }

    public void setAgentsConfig(final MultiSubnodeConfiguration agentsConfig) {
        this.agentsConfig = agentsConfig;
    }

    public Map<String, String> getAgentClassNames() {
        return agentClassNames;
    }

    public void setAgentClassNames(final Map<String, String> agentClassNames) {
        this.agentClassNames = agentClassNames;
    }

    public class Agent {

        protected MultiSubnodeConfiguration funnelConfig;
        protected Map<String, String> funnelClassNames; // <id, class>
        protected Map<String, FunnelComponents> funnelComponents;
        private String fileManagerConfig;
        private String fileBookKeeperConfig;


        public MultiSubnodeConfiguration getFunnelConfig()
        {
            return funnelConfig;
        }

        public void setFileManagerConfig(final String fileManagerConfig) {
            this.fileManagerConfig = fileManagerConfig;
        }

        public String getFileManagerConfig() {
            return fileManagerConfig;
        }

        public String getFileBookKeeperConfig() {
            return fileBookKeeperConfig;
        }

        public void setFileBookKeeperConfig(final String fileBookKeeperConfig) {
            this.fileBookKeeperConfig = fileBookKeeperConfig;
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
}
