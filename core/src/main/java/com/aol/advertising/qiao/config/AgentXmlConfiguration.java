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

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.OverrideCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.config.PropertyValue.DataType;
import com.aol.advertising.qiao.config.PropertyValue.Type;
import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.XmlConfigUtil;

/**
 * AgentXmlConfiguration parses application's qiao.xml and loads configuration
 * parameters for all the defined components.
 */
public class AgentXmlConfiguration implements IAgentXmlConfig
{

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected String configXmlFileUri;
    protected String configPropertyFiles;
    protected String schemaLocationUri = "classpath:qiao-config.xsd";
    protected HierarchicalConfiguration xmlConfig;

    protected QiaoConfig qiaoConfig;


    @Override
    public void load() throws ConfigurationException
    {
        boolean valid = XmlConfigUtil.validateXml(configXmlFileUri,
            schemaLocationUri);

        if (!valid)
            throw new ConfigurationException(
                "Invalid Qiao configuration file: " + configXmlFileUri);

        xmlConfig = readConfigurationFiles(configXmlFileUri,
            configPropertyFiles);

        logger.info(configXmlFileUri + " loaded");

        loadQiaoConfig();
    }


    private void loadQiaoConfig()
    {
        qiaoConfig = new QiaoConfig();

        MultiSubnodeConfiguration agentsConfig = loadMutiNodeConfigAndProperties(ConfigConstants.CFGKEY_AGENT);
        if (agentsConfig == null) {
            logger.warn(ConfigConstants.CFGKEY_AGENT + " not configured");
        } else {

            qiaoConfig.setAgentsConfig(agentsConfig);
            Map<String, String> classnames = getClassNames(agentsConfig);
            qiaoConfig.setAgentClassNames(classnames);
            Map<String, QiaoConfig.Agent> agents = new HashMap<>();

            for (int i = 0; i < agentsConfig.size(); i++) {
                QiaoConfig.Agent agent = qiaoConfig.new Agent();
                String funnels = String.format(ConfigConstants.CFGKEY_FUNNEL, i);
                MultiSubnodeConfiguration tmpFunnels = loadMutiNodeConfigAndProperties(funnels);

                if (tmpFunnels == null) {
                    logger.warn(ConfigConstants.CFGKEY_FUNNEL + " not configured");
                } else {
                    agent.setFunnelConfig(tmpFunnels);
                    Map<String, String> classnamesFunnels = getClassNames(tmpFunnels);
                    agent.setFunnelClassNames(classnamesFunnels);
                    Map<String, QiaoConfig.Agent.FunnelComponents> mcomponents = new HashMap<>();

                    for (int j = 0; j < tmpFunnels.size(); j++) {
                        QiaoConfig.Agent.FunnelComponents fc = agent.new FunnelComponents();
                        fc.setSourceConfig(loadSourceConfig(i, j));
                        fc.setSinkConfig(loadSinkConfig(i, j));
                        fc.setId(tmpFunnels.get(j).getId());
                        mcomponents.put(fc.getId(), fc);
                    }

                    agent.setFunnelComponents(mcomponents);
                }
                String statsManager = String.format(ConfigConstants.CFGKEY_FILE_MANAGER, i);
                SingleSubnodeConfiguration tmpStatConfiguration = loadSingleNodeConfigAndProperties(statsManager);
                QiaoConfig.Agent.FileManagerConfig fileManagerConfig = agent.new FileManagerConfig();
                fileManagerConfig.setFileManagerConfiguration(tmpStatConfiguration);

                String doneFileHandler = String.format(ConfigConstants.CFGKEY_FILE_MANAGER_DONE_FILE_HANDLER, i);
                SingleSubnodeConfiguration doneFileHandlerConfiguration = loadSingleNodeConfigAndProperties(doneFileHandler);
                fileManagerConfig.setDoneFileHandlerConfiguration(doneFileHandlerConfiguration);

                String quarantineFileHandler = String.format(ConfigConstants.CFGKEY_FILE_MANAGER_QUARANTINE_FILE_HANDLER, i);
                SingleSubnodeConfiguration quarantineFileHandlerConfiguration = loadSingleNodeConfigAndProperties
                    (quarantineFileHandler);
                fileManagerConfig.setQuarantineFileHandlerConfiguration(quarantineFileHandlerConfiguration);

                agent.setFileManagerConfig(fileManagerConfig);

                String fileBookKepper = String.format(ConfigConstants.CFGKEY_FILE_BOOK_KEPPER, i);
                SingleSubnodeConfiguration fileBookKeeperConfiguration = loadSingleNodeConfigAndProperties
                    (fileBookKepper);
                fileManagerConfig.setFileBookKepperConfiguration(fileBookKeeperConfiguration);

                agent.setFileManagerConfig(fileManagerConfig);

                agents.put(agentsConfig.get(i).getId(), agent);
            }
            qiaoConfig.setAgents(agents);
        }
    }


    protected InjectorConfig loadSourceConfig(int idx, final int idxFunnel)
    {
        String node_idx = String.format(ConfigConstants.CFGKEY_FUNNEL_INJECTOR,
            idx,idxFunnel);
        InjectorConfig source_cfg = new InjectorConfig();
        SingleSubnodeConfiguration tmp = loadSingleNodeConfigAndProperties(node_idx);
        if (tmp == null)
        {
            logger.warn(node_idx + " not configured");
        }
        else
        {
            if (xmlConfig.getMaxIndex(node_idx + ".doneFileHandler") != -1){
                SingleSubnodeConfiguration tmpDoneFileHander = loadSingleNodeConfigAndProperties(node_idx + ".doneFileHandler");
                if (tmpDoneFileHander != null)
                    source_cfg.setDoneFileHander(tmpDoneFileHander);
            }

            source_cfg.setSourceConfig(tmp);
            source_cfg.setId(tmp.getId());
            source_cfg.setSourceClassName((String) tmp
                .getAttribute(ConfigConstants.CFGATTR_CLASSNAME));
        }

        return source_cfg;
    }


    protected EmitterConfig loadSinkConfig(int idx, final int idxFunnel)
    {
        String node_idx = String.format(ConfigConstants.CFGKEY_FUNNEL_EMITTER,
            idx,idxFunnel);

        EmitterConfig sink_cfg = new EmitterConfig();
        sink_cfg.setEmitterContainerClassName(ConfigConstants.DEFAULT_FUNNEL_EMITTERCONTAINER_CLASSNAME);

        MultiSubnodeConfiguration tmp = loadMutiNodeConfigAndProperties(node_idx);
        if (tmp == null)
        {
            logger.warn(node_idx + " not configured");
        }
        else
        {
            sink_cfg.setEmitterConfig(tmp);
            Map<String, String> classnames = getClassNames(tmp);
            sink_cfg.setEmitterClassNames(classnames);
        }

        return sink_cfg;
    }


    private Map<String, String> getClassNames(
        MultiSubnodeConfiguration clsConfig)
    {
        if (clsConfig == null)
            return null;

        Map<String, String> ans = new HashMap<String, String>(clsConfig.size());
        for (int i = 0; i < clsConfig.size(); i++)
        {
            Map<String, Object> m = clsConfig.get(i);
            ans.put((String) m.get(ConfigConstants.CFGATTR_ID),
                (String) m.get(ConfigConstants.CFGATTR_CLASSNAME));
        }

        return ans;
    }


    /**
     * Load configuration file and interpolate variables. Note that due to the
     * way Apache Commons Configuration treats property keys, user should avoid
     * using dot ('.') in the property key. Otherwise ACC may not substitute
     * some value if the variable contains valid element name(s).
     *
     * @param xmlConfigFile
     * @param propConfigFiles
     * @return
     * @throws ConfigurationException
     */
    protected HierarchicalConfiguration readConfigurationFiles(
        String xmlConfigFile, String propConfigFiles)
        throws ConfigurationException
    {
        try
        {
            // convert URI to URL
            URL[] prop_urls = null;
            URL xml_url = CommonUtils.uriToURL(xmlConfigFile);
            if (propConfigFiles != null)
            {
                String[] prop_files = propConfigFiles.split(",");
                prop_urls = new URL[prop_files.length];
                for (int i = 0; i < prop_files.length; i++)
                {
                    prop_urls[i] = CommonUtils.uriToURL(prop_files[i]);
                }
            }

            // combine xml and properties configurations
            CombinedConfiguration combined_cfg = new CombinedConfiguration(
                new OverrideCombiner());

            XMLConfiguration cfg_xml = new XMLConfiguration();
            cfg_xml.setDelimiterParsingDisabled(true);
            cfg_xml.load(xml_url);

            combined_cfg.addConfiguration(cfg_xml);

            if (prop_urls != null)
            {
                // properties in the earlier files take precedence if duplicate
                for (int i = 0; i < prop_urls.length; i++)
                {
                    PropertiesConfiguration cfg_props = new PropertiesConfiguration();
                    cfg_props.setDelimiterParsingDisabled(false);

                    cfg_props.load(prop_urls[i]);

                    combined_cfg.addConfiguration(cfg_props);

                }
            }

            HierarchicalConfiguration config = (HierarchicalConfiguration) combined_cfg
                .interpolatedConfiguration(); // !!! resolve variables

            return config;
        }
        catch (Exception e)
        {
            throw new ConfigurationException(e.getMessage(), e);
        }

    }


    /**
     * Parse xml and build an config object. The returned object preserves the
     * order the elements appears in the xml files.
     *
     * @param key
     * @return
     */
    private MultiSubnodeConfiguration loadMutiNodeConfigAndProperties(String key)
    {

        logger.debug("------- looking up " + key + " -------");

        List<Map<String, Object>> list = XmlConfigUtil
            .parseMultiNodesAttributes(xmlConfig, key);
        if (list == null || list.size() == 0)
            return null;

        MultiSubnodeConfiguration c = new MultiSubnodeConfiguration();

        for (int i = 0; i < list.size(); i++)
        {
            SingleSubnodeConfiguration single = new SingleSubnodeConfiguration();
            Map<String, Object> member = list.get(i);
            single.putAll(member);
            single.setId((String) member.get(ConfigConstants.CFGATTR_ID));

            Map<String, PropertyValue> prop_map = getPropertyMap(String.format(
                ConfigConstants.CFGKEY_PROPERTY_TEMPLATE, key, i));

            if (prop_map != null && prop_map.size() > 0)
            {
                // "properties" -> map of <name, value> pair
                single.setProperties(prop_map);
            }
            c.add(single);
        }

        return c;
    }


    private SingleSubnodeConfiguration loadSingleNodeConfigAndProperties(
        String key)
    {

        logger.debug("------- looking up " + key + " -------");

        Map<String, Object> member = XmlConfigUtil.parseSingleNodeAttributes(
            xmlConfig, key);
        if (member == null || member.size() == 0)
            return null;

        SingleSubnodeConfiguration single = new SingleSubnodeConfiguration();
        single.putAll(member);
        single.setId((String) member.get(ConfigConstants.CFGATTR_ID));

        Map<String, PropertyValue> prop_map = getPropertyMap(String.format(
            ConfigConstants.CFGKEY_PROPERTY_TEMPLATE, key, 0));

        if (prop_map != null && prop_map.size() > 0)
        {
            // "properties" -> map of <name, value> pair
            single.setProperties(prop_map);
        }

        return single;
    }


    private Map<String, PropertyValue> getPropertyMap(String key)
    {

        List<Map<String, Object>> list = XmlConfigUtil
            .parseMultiNodesAttributes(xmlConfig, key);

        if (list == null || list.size() == 0)
            return null;

        Map<String, PropertyValue> result = new HashMap<String, PropertyValue>();

        for (int i = 0; i < list.size(); i++)
        {
            Map<String, Object> props = list.get(i);
            PropertyValue pv = new PropertyValue();
            String name = (String) props.get(ConfigConstants.CFGATTR_PROP_NAME);
            String value = (String) props
                .get(ConfigConstants.CFGATTR_PROP_VALUE);
            if (value != null)
            {
                pv.setName(name);
                pv.setValue(value);
                pv.setType(Type.IS_VALUE);

                String value_type = (String) props
                    .get(ConfigConstants.CFGATTR_PROP_TYPE);
                if (value_type != null)
                {
                    DataType dtype = DataType.find(value_type);
                    if (dtype == null)
                    {
                        String msg = "invalid value type" + value_type;
                        logger.error(msg);
                        throw new ConfigurationException(msg);
                    }

                    pv.setDataType(dtype);
                }
                else
                {
                    pv.setDataType(DataType.STRING); // default: string
                }

                String default_value = (String) props
                    .get(ConfigConstants.CFGATTR_PROP_DEFAULT);
                if (default_value != null)
                {
                    pv.setDefaultValue(default_value);
                }

            }
            else
            {
                String ref = (String) props
                    .get(ConfigConstants.CFGATTR_PROP_REF);
                if (ref != null)
                {
                    pv.setName(name);
                    pv.setValue(ref);
                    pv.setType(Type.IS_REF);
                }
            }

            result.put(name, pv);
        }

        return result;
    }


    private String lookupClassName(String nodeName)
    {
        SingleSubnodeConfiguration c = loadNodeConfig(nodeName);
        return (String) c.get(ConfigConstants.CFGATTR_CLASSNAME);
    }


    private SingleSubnodeConfiguration loadNodeConfig(String key)
    {

        logger.debug("------- looking up " + key + " -------");
        Map<String, Object> map = XmlConfigUtil.parseSingleNodeAttributes(
            xmlConfig, key);

        if (logger.isDebugEnabled())
        {
            for (Map.Entry<String, Object> kv : map.entrySet())
            {
                logger.debug(kv.getKey() + "=" + kv.getValue());
            }

        }

        SingleSubnodeConfiguration single = new SingleSubnodeConfiguration();
        single.putAll(map);
        single.setId((String) map.get(ConfigConstants.CFGATTR_ID));

        return single;
    }


    public void setConfigXmlFileUri(String configXmlFileUri)
    {
        this.configXmlFileUri = configXmlFileUri;
    }


    public void setConfigPropertyFiles(String configPropertyFiles)
    {
        this.configPropertyFiles = configPropertyFiles;
    }


    public void setSchemaLocationUri(String schemaLocationUri)
    {
        this.schemaLocationUri = schemaLocationUri;
    }


    public String getConfigXmlFileUri()
    {
        return configXmlFileUri;
    }


    public String getConfigPropertyFiles()
    {
        return configPropertyFiles;
    }


    public String getSchemaLocationUri()
    {
        return schemaLocationUri;
    }


    public QiaoConfig getQiaoConfig()
    {
        return qiaoConfig;
    }

}