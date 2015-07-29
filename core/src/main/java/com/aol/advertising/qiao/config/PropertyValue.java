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
