/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2011-2012 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   XmlConfigUtil.java	
 * Description:
 * @author:     ytung
 * @version:    1.1
 *
 ****************************************************************************/

package com.aol.advertising.qiao.util;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXParseException;

public class XmlConfigUtil
{
    private static Logger logger = LoggerFactory.getLogger(XmlConfigUtil.class);


    /**
     * Validate an XML document against the given schema.
     * 
     * @param xmlFileLocationUri
     *            Location URI of the document to be validated.
     * @param schemaLocationUri
     *            Location URI of the XML schema in W3C XML Schema Language.
     * @return true if valid, false otherwise.
     */
    public static boolean validateXml(String xmlFileLocationUri,
            String schemaLocationUri)
    {
        SchemaFactory factory = SchemaFactory
                .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

        InputStream is = null;
        try
        {
            Schema schema = factory.newSchema(CommonUtils
                    .uriToURL(schemaLocationUri));
            Validator validator = schema.newValidator();
            URL url = CommonUtils.uriToURL(xmlFileLocationUri);
            is = url.openStream();
            Source source = new StreamSource(is);

            validator.validate(source);
            logger.info(">> successfully validated configuration file: "
                    + xmlFileLocationUri);

            return true;
        }
        catch (SAXParseException e)
        {
            logger.error(e.getMessage() + " (line:" + e.getLineNumber()
                    + ", col:" + e.getColumnNumber() + ")");

        }
        catch (Exception e)
        {
            if (e instanceof SAXParseException)
            {
                SAXParseException e2 = (SAXParseException) e;
                logger.error(e2.getMessage() + "at line:" + e2.getLineNumber()
                        + ", col:" + e2.getColumnNumber());
            }
            else
            {
                logger.error(e.getMessage());
            }
        }
        finally
        {
            IOUtils.closeQuietly(is);
        }

        return false;
    }


    /**
     * Parse xml configuration and return a list of attributes belonging to one
     * or more elements with the specific key. It returns a list preserving the
     * order of elements in xml.
     * 
     * @param config
     * @param key
     *            identifies xml elements with the specific tag
     * @return
     */
    public static List<Map<String, Object>> parseMultiNodesAttributes(
            HierarchicalConfiguration config, String key)
    {
        List<Map<String, Object>> result = null;

        // get a list of subnode configurations for all configuration nodes
        // selected by the given key, e.g. "node.inbound.listener"
        List< ? > list = config.configurationsAt(key);
        if (list != null)
        {
            result = new ArrayList<Map<String, Object>>();
            for (Iterator< ? > iter = list.iterator(); iter.hasNext();)
            {
                HierarchicalConfiguration sub = (HierarchicalConfiguration) iter
                        .next();
                Node root = sub.getRoot();

                Map<String, Object> attributes = new HashMap<String, Object>(
                        root.getAttributeCount());

                // get attribute nodes
                List< ? > attrs = root.getAttributes();
                for (Object attr : attrs)
                {
                    ConfigurationNode nd = (ConfigurationNode) attr;
                    attributes.put(nd.getName(), nd.getValue());
                }
                result.add(attributes);
            }
        }

        return result;
    }


    public static Map<String, Object> parseSingleNodeAttributes(
            HierarchicalConfiguration config, String key)
    {
        Map<String, Object> result = null;

        // get a subnode configuration for the node specified by the given key
        SubnodeConfiguration c = config.configurationAt(key);
        if (c != null)
        {
            Node root = c.getRoot();
            result = new HashMap<String, Object>(root.getAttributeCount());

            List< ? > attrs = root.getAttributes();
            for (Object attr : attrs)
            {
                ConfigurationNode nd = (ConfigurationNode) attr;
                result.put(nd.getName(), nd.getValue());
            }
        }

        return result;
    }


    public static void walkTree(ConfigurationNode node)
    {
        if (node != null)
        {
            logger.info("node name=" + node.getName() + ", #childrens="
                    + node.getChildrenCount());
            List<ConfigurationNode> childrens = node.getChildren();
            for (ConfigurationNode sub : childrens)
                walkTree(sub);
        }
    }
}
