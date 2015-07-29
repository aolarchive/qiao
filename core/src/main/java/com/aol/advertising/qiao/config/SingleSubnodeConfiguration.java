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

/**
 * Attribute map for a node in an xml configuration file.
 */
public class SingleSubnodeConfiguration extends HashMap<String, Object>
{
    private static final long serialVersionUID = 4659676278541441107L;
    private String id;
    private Map<String, PropertyValue> properties;

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    // @SuppressWarnings("unchecked")
    public Map<String, PropertyValue> getProperties()
    {
        // return (Map<String, PropertyValue>) get(Constants.CFGKEY_PROPERTIES);
        return properties;
    }

    public Object getAttribute(String key)
    {
        return get(key);
    }

    public void setProperties(Map<String, PropertyValue> properties)
    {
        this.properties = properties;
    }
}
