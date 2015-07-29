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

package com.aol.advertising.qiao.management.jmx;

import java.io.FileNotFoundException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.util.Log4jConfigurer;

@ManagedResource(description = "LOG4j Level Changer")
public class Log4jLevelControl
{
    private static final Logger log = Logger.getLogger(Log4jLevelControl.class);
    private String log4jLocation;

    @ManagedOperation(description = "Set the logging level for a category")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "category", description = "Logger category"),
            @ManagedOperationParameter(name = "level", description = "Logging level") })
    public void setLogLevel(String category, String level)
    {
        LogManager.getLogger(category).setLevel(Level.toLevel(level));
    }

    @ManagedOperation(description = "Get the logging level for a category")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "category", description = "Logger category") })
    public String getLoggerLevel(String category)
    {
        Level lvl = LogManager.getLogger(category).getEffectiveLevel();
        return lvl == null ? "null" : lvl.toString();
    }

    @ManagedAttribute
    public String getRootLoggerLevel()
    {
        return LogManager.getRootLogger().getLevel().toString();
    }

    @ManagedOperation(description = "Set the rootlogger's logging level")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "level", description = "Logging level") })
    public void setRootLoggerLevel(String level)
    {
        LogManager.getRootLogger().setLevel(Level.toLevel(level));
    }

    @ManagedOperation(description = "Reset logging from the log4j.properties file")
    public boolean refreshLogger()
    {
        if (log4jLocation == null)
        {
            log.warn("log4jLocation not set");
            return false;
        }

        try
        {
            Log4jConfigurer.initLogging(log4jLocation);
            return true;
        }
        catch (FileNotFoundException e)
        {
            return false;
        }

    }

    @ManagedAttribute
    public String getLog4jLocation()
    {
        return log4jLocation;
    }

    @ManagedOperation(description = "Set log4j.properties location")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "log4jLocation", description = "The location of log4j.properties") })
    public void setLog4jLocation(String log4jLocation)
    {
        this.log4jLocation = log4jLocation;
    }

}
