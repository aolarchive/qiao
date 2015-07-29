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

public class EmitterConfig
{
    protected String emitterContainerClassName;
    protected Map<String, String> emitterClassNames; // <id, class>
    protected MultiSubnodeConfiguration emitterConfig;


    public String getEmitterContainerClassName()
    {
        return emitterContainerClassName;
    }


    public void setEmitterContainerClassName(String sinkContainerClassName)
    {
        this.emitterContainerClassName = sinkContainerClassName;
    }


    public Map<String, String> getEmitterClassNames()
    {
        return emitterClassNames;
    }


    public void setEmitterClassNames(Map<String, String> sinkClassNames)
    {
        this.emitterClassNames = sinkClassNames;
    }


    public MultiSubnodeConfiguration getSinkConfig()
    {
        return emitterConfig;
    }


    public void setEmitterConfig(MultiSubnodeConfiguration sinkConfig)
    {
        this.emitterConfig = sinkConfig;
    }

}
