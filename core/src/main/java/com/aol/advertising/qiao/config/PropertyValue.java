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
 * File Name:   PropertyValue.java	
 * Description:
 * @author:     ytung
 * @version:    1.1
 *
 ****************************************************************************/

package com.aol.advertising.qiao.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertyValue
{
    protected static final Pattern PATTERN_UNRESOLVED_VALUE = Pattern
            .compile("\\s*\\$\\{.*\\}\\s*");

    public enum Type
    {
        IS_VALUE("value"), IS_REF("ref");

        String v;


        Type(String value)
        {
            this.v = value;
        }


        public static Type find(String s)
        {
            for (Type c : Type.values())
            {
                if (c.v.equals(s))
                    return c;
            }

            return null;
        }
    };

    public enum DataType
    {
        SHORT("short", Short.TYPE), INTEGER("int", Integer.TYPE), LONG("long",
                Long.TYPE), // 
        FLOAT("float", Float.TYPE), DOUBLE("double", Double.TYPE), // 
        BOOLEAN("boolean", Boolean.TYPE), STRING("string", String.class), //
        CLZ_SHORT("Short", Short.class), CLZ_INTEGER("Integer", Integer.class), CLZ_LONG(
                "Long", Long.class), //
        CLZ_FLOAT("Float", Float.class), CLZ_DOUBLE("Double", Double.class), CLZ_BOOLEAN(
                "Boolean", Boolean.class);

        String v;
        Class< ? > javaClass;


        DataType(String value, Class< ? > clzz)
        {
            this.v = value;
            this.javaClass = clzz;
        }


        public static DataType find(String s)
        {
            for (DataType c : DataType.values())
            {
                if (c.v.equals(s))
                    return c;
            }

            return null;
        }


        public Class< ? > getJavaClass()
        {
            return javaClass;
        }


        public String toString()
        {
            return v;
        }

    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    String name;
    String value;
    DataType dataType;
    Type type;
    Object refObject;
    String defaultValue;


    public String getName()
    {
        return name;
    }


    public void setName(String name)
    {
        this.name = name;
    }


    public String getValue()
    {
        return value;
    }


    public void setValue(String value)
    {
        this.value = value;
    }


    public Type getType()
    {
        return type;
    }


    public void setType(Type type)
    {
        this.type = type;
    }


    public Object getRefObject()
    {
        return refObject;
    }


    public void setRefObject(Object refObject)
    {
        this.refObject = refObject;
    }


    public DataType getDataType()
    {
        return dataType;
    }


    public void setDataType(DataType dataType)
    {
        this.dataType = dataType;
    }


    public String getDefaultValue()
    {
        return defaultValue;
    }


    public void setDefaultValue(String defaultValue)
    {
        this.defaultValue = defaultValue;
    }


    public boolean isResolved()
    {
        Matcher m = PATTERN_UNRESOLVED_VALUE.matcher(value);
        return !m.matches();
    }
}
