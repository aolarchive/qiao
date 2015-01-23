/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2011 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   ContextUtil.java	
 * Description:
 * @author:     ytung
 * @version:    1.0
 *
 ****************************************************************************/

package com.aol.advertising.qiao.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.ReflectionUtils;

import com.aol.advertising.qiao.config.PropertyValue;
import com.aol.advertising.qiao.config.PropertyValue.DataType;
import com.aol.advertising.qiao.config.PropertyValue.Type;
import com.aol.advertising.qiao.config.SingleSubnodeConfiguration;
import com.aol.advertising.qiao.exception.BeanNotFoundException;
import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.exception.MethodNotFoundException;

/**
 * ContextUtil needs to be defined as a singleton.
 */
public class ContextUtils implements ApplicationContextAware
{
    protected static Logger logger = LoggerFactory.getLogger(ContextUtils.class);
    protected ApplicationContext applicationContext;
    protected static ContextUtils _instance;

    protected static final String ERR_METHOD_NOTFOUND = "Unable to find method %s with param type %s in the class %s";


    public static Object createBean(String className)
            throws ClassNotFoundException
    {
        //logger.info("loading " + className + "...");
        Object o = null;

        logger.info("Instantiate the class " + className);
        Class< ? > clz = Class.forName(className);
        try
        {
            o = clz.newInstance();
        }
        catch (Exception e)
        {
            logger.error("Failed to instantiate class " + className);
        }

        return o;
    }


    public static Object createBeanSilently(String className)
            throws ClassNotFoundException
    {
        Object o = null;

        Class< ? > clz = Class.forName(className);
        try
        {
            o = clz.newInstance();
        }
        catch (Exception e)
        {
            logger.error("Failed to instantiate class " + className);
        }

        return o;
    }


    public static Object loadClass(ApplicationContext appContext,
            String className) throws BeansException, ClassNotFoundException
    {
        return appContext.getBean(Class.forName(className));
    }


    public static Object loadClass(String className) throws BeansException,
            ClassNotFoundException
    {
        if (_instance == null)
        {
            throw new FatalBeanException("Missing ContextUtil bean");
        }

        return _instance.applicationContext.getBean(Class.forName(className));
    }


    public static Object loadClassById(String id) throws BeansException,
            ClassNotFoundException
    {
        if (_instance == null)
        {
            throw new FatalBeanException("Missing ContextUtil bean");
        }

        return _instance.applicationContext.getBean(id);
    }


    public static <T> T getBean(Class<T> clz)
    {
        if (_instance == null)
        {
            throw new FatalBeanException("Missing ContextUtil bean");
        }

        return _instance.applicationContext.getBean(clz);
    }


    public static String[] getBeanNames(Class< ? > clz)
    {
        if (_instance == null)
        {
            throw new FatalBeanException("Missing ContextUtil bean");
        }

        return BeanFactoryUtils.beanNamesForTypeIncludingAncestors(
                _instance.applicationContext, clz);

    }


    /**
     * Locate a Spring bean of the given class. If not found, instantiate a new
     * instance of the class.
     * 
     * @param clzname
     *            class name
     * @return the object of the class
     * @throws ClassNotFoundException
     */
    public static Object getOrCreateBean(String clzname)
            throws ClassNotFoundException
    {
        Object o = null;
        Class< ? > clz = Class.forName(clzname);
        try
        {
            o = ContextUtils.getBean(clz);
        }
        catch (NoSuchBeanDefinitionException e)
        {
            try
            {
                o = clz.newInstance();
            }
            catch (Exception e1)
            {
                logger.error("Failed to instantiate class " + clzname);
            }
        }

        return o;
    }


    public static void resolvePropertyReferences(
            SingleSubnodeConfiguration singleNode) throws BeansException,
            ClassNotFoundException
    {
        Map<String, PropertyValue> map = singleNode.getProperties();
        if (map == null || map.size() == 0)
            return;

        ContextUtils.resolvePropertyReferences(map);

    }


    public static void resolvePropertyReferences(
            Map<String, PropertyValue> propsMap) throws BeansException,
            ClassNotFoundException
    {
        if (propsMap == null || propsMap.size() == 0)
            return;

        for (Entry<String, PropertyValue> entry : propsMap.entrySet())
        {
            PropertyValue pv = entry.getValue();
            if (pv.getType() == Type.IS_REF)
            {
                Object o = ContextUtils.loadClassById(pv.getValue());
                if (o == null)
                {
                    String err = "Unable to find bean id=" + pv.getValue();
                    logger.error(err);
                    throw new BeanNotFoundException(err);
                }

                pv.setRefObject(o);
            }
        }
    }


    protected static Object stringToValueByType(String str, DataType type)
    {

        switch (type)
        {
            case STRING:
                return str;
            case INTEGER:
                return Integer.parseInt(str);
            case CLZ_INTEGER:
                return Integer.valueOf(str);
            case SHORT:
                return Short.parseShort(str);
            case CLZ_SHORT:
                return Short.valueOf(str);
            case LONG:
                return Long.parseLong(str);
            case CLZ_LONG:
                return Long.valueOf(str);
            case FLOAT:
                return Float.parseFloat(str);
            case CLZ_FLOAT:
                return Float.valueOf(str);
            case DOUBLE:
                return Double.parseDouble(str);
            case CLZ_DOUBLE:
                return Double.valueOf(str);
            case BOOLEAN:
                return Boolean.parseBoolean(str);
            case CLZ_BOOLEAN:
                return Boolean.valueOf(str);
            default: // default to string   
                return str;
        }

    }


    public static void injectMethods(Object object,
            SingleSubnodeConfiguration cfg) throws BeansException,
            ClassNotFoundException
    {
        Map<String, PropertyValue> props = cfg.getProperties();
        // <fieldname, fieldvalue>

        ContextUtils.injectMethods(object, props);

    }


    public static void injectMethods(Object object,
            Map<String, PropertyValue> props) throws BeansException,
            ClassNotFoundException
    {
        resolvePropertyReferences(props);

        if (props != null)
        {
            for (String key : props.keySet())
            {
                String mth_name = "set" + StringUtils.capitalize(key);

                PropertyValue pv = props.get(key);
                if (pv.getType() == Type.IS_VALUE)
                {
                    String pv_value = pv.getValue();
                    if (!pv.isResolved())
                    {
                        if (pv.getDefaultValue() == null) // not resolved + no default
                            throw new ConfigurationException("value "
                                    + pv.getValue() + " not resolved");

                        pv_value = pv.getDefaultValue();
                    }

                    Method mth = findMethod(object.getClass(), mth_name, pv
                            .getDataType().getJavaClass());
                    if (mth != null)
                    {
                        ReflectionUtils.invokeMethod(
                                mth,
                                object,
                                ContextUtils.stringToValueByType(pv_value,
                                        pv.getDataType()));
                    }
                    else
                    {
                        String err = String.format(ERR_METHOD_NOTFOUND,
                                mth_name, pv.getDataType(), object.getClass()
                                        .getSimpleName());
                        logger.error(err);
                        throw new MethodNotFoundException(err);

                    }
                }
                else
                {
                    Method mth = findMethod(object.getClass(), mth_name, pv
                            .getRefObject().getClass());

                    if (mth != null)
                    {
                        ReflectionUtils.invokeMethod(mth, object,
                                pv.getRefObject());
                    }
                    else
                    {
                        String err = String.format(ERR_METHOD_NOTFOUND,
                                mth_name, pv.getRefObject().getClass()
                                        .getSimpleName(), object.getClass()
                                        .getSimpleName());
                        logger.error(err);
                        throw new MethodNotFoundException(err);
                    }
                }
            }
        }
    }


    /**
     * A simple call to invoke a class method.
     * 
     * @param object
     *            the target object to invoke the method on
     * @param fieldName
     *            the field whose set method to be invoked
     * @param param
     *            the invocation argument
     * @throws BeansException
     * @throws ClassNotFoundException
     */
    public static void injectMethod(Object object, String fieldName,
            Object param) throws BeansException, ClassNotFoundException
    {
        String mth_name = "set" + StringUtils.capitalize(fieldName);

        Method mth = findMethod(object.getClass(), mth_name, param.getClass());

        if (mth != null)
        {
            ReflectionUtils.invokeMethod(mth, object, param);
        }
        else
        {
            String err = String.format(ERR_METHOD_NOTFOUND, mth_name, param
                    .getClass().getSimpleName(), object.getClass()
                    .getSimpleName());
            logger.error(err);
            throw new MethodNotFoundException(err);
        }
    }


    /**
     * Invoke a class method if exists.
     * 
     * @param object
     * @param fieldName
     * @param param
     * @throws BeansException
     * @throws ClassNotFoundException
     */
    public static void injectMethodSilently(Object object, String fieldName,
            Object param) throws BeansException, ClassNotFoundException
    {
        String mth_name = "set" + StringUtils.capitalize(fieldName);

        Method mth = findMethod(object.getClass(), mth_name, param.getClass());

        if (mth != null)
        {
            ReflectionUtils.invokeMethod(mth, object, param);
        }
    }


    /**
     * Supporting one parameter only.
     * 
     * @param clazz
     * @param methodName
     * @param paramType
     * @return
     */
    private static Method findMethod(Class< ? > clazz, String methodName,
            Class< ? > paramType)
    {
        // trace
        //System.out.println("findMethod> class=" + clazz.getSimpleName()
        //        + ",method=" + methodName + ",param="
        //        + paramType.getSimpleName());
        Method mth = ReflectionUtils.findMethod(clazz, methodName, paramType);
        if (mth == null)
        {

            // check param's interfaces
            Class< ? >[] list = paramType.getInterfaces();
            if (list != null && list.length > 0)
            {
                for (Class< ? > c : list)
                {
                    mth = findMethod(clazz, methodName, c);
                    if (mth != null)
                        return mth;
                }
            }

            // check param's super class
            Class< ? > param_super = paramType.getSuperclass();
            if (param_super != null)
            {
                mth = findMethod(clazz, methodName, param_super);
                if (mth != null)
                    return mth;
            }

            if (mth == null)
            {
                // check object's super class
                Class< ? > parent_clz = clazz.getSuperclass();
                if (parent_clz != null)
                {
                    mth = findMethod(parent_clz, methodName, paramType);
                }
            }
        }

        return mth;
    }


    public static <T> Map<String, T> getBeansOfType(Class<T> type,
            boolean includeNonSingletons, boolean allowEagerInit)
            throws BeansException
    {
        return _instance.applicationContext.getBeansOfType(type,
                includeNonSingletons, allowEagerInit);
    }


    // ----------------------------------------------------

    public static ContextUtils getInstance()
    {
        if (_instance == null)
            _instance = new ContextUtils();

        return _instance;
    }


    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException
    {
        this.applicationContext = applicationContext;
    }


    public ApplicationContext getApplicationContext()
    {
        return applicationContext;
    }

}
