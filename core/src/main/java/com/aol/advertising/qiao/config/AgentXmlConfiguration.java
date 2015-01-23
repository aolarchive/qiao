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
 * File Name:   AgentXmlConfiguration.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.config;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.OverrideCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.config.PropertyValue.DataType;
import com.aol.advertising.qiao.config.PropertyValue.Type;
import com.aol.advertising.qiao.config.QiaoConfig.FunnelComponents;
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

        MultiSubnodeConfiguration tmp = loadMutiNodeConfigAndProperties(ConfigConstants.CFGKEY_FUNNEL);
        if (tmp == null)
        {
            logger.warn(ConfigConstants.CFGKEY_FUNNEL + " not configured");
        }
        else
        {
            qiaoConfig.setFunnelConfig(tmp);
            Map<String, String> classnames = getClassNames(tmp);
            qiaoConfig.setFunnelClassNames(classnames);

            Map<String, FunnelComponents> mcomponents = new HashMap<String, FunnelComponents>();
            for (int i = 0; i < tmp.size(); i++)
            {
                FunnelComponents fc = qiaoConfig.new FunnelComponents();

                fc.setSourceConfig(loadSourceConfig(i));
                fc.setSinkConfig(loadSinkConfig(i));
                fc.setId(tmp.get(i).getId());
                mcomponents.put(fc.getId(), fc);
            }

            qiaoConfig.setFunnelComponents(mcomponents);
        }
    }


    protected InjectorConfig loadSourceConfig(int idx)
    {
        String node_idx = String.format(ConfigConstants.CFGKEY_FUNNEL_INJECTOR,
                idx);
        InjectorConfig source_cfg = new InjectorConfig();
        SingleSubnodeConfiguration tmp = loadSingleNodeConfigAndProperties(node_idx);
        if (tmp == null)
        {
            logger.warn(node_idx + " not configured");
        }
        else
        {
            source_cfg.setSourceConfig(tmp);
            source_cfg.setId(tmp.getId());
            source_cfg.setSourceClassName((String) tmp
                    .getAttribute(ConfigConstants.CFGATTR_CLASSNAME));
        }

        return source_cfg;
    }


    protected EmitterConfig loadSinkConfig(int idx)
    {
        String node_idx = String.format(ConfigConstants.CFGKEY_FUNNEL_EMITTER,
                idx);

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
            cfg_xml.setAttributeSplittingDisabled(true);
            cfg_xml.load(xml_url);

            combined_cfg.addConfiguration(cfg_xml);

            if (prop_urls != null)
            {
                // properties in the earlier files take precedence if duplicate
                for (int i = 0; i < prop_urls.length; i++)
                {
                    PropertiesConfiguration cfg_props = new PropertiesConfiguration();
                    cfg_props.setDelimiterParsingDisabled(true);
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
