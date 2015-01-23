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
 * File Name:   EnumCommand.java    
 * Description:
 * @author:     ytung
 * @version:    1.0
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management.jmx;

public enum EnumCommand
{
    // agent
    START_AGENT("!start_agent!"), // 
    SUSPEND_AGENT("!suspend_agent!"), // 
    RESUME_AGENT("!resume_agent!"), //
    // connections
    RESET_INBOUND_CONNECTION("!reset_inbound_connection!"), //
    RESET_OUTBOUND_CONNECTION("!reset_outbound_connection!"), //
    // stats processing
    START_STATS("!start_stats!"), //
    STOP_STATS("!stop_stats!"), //
    RESET_STATS("!reset_stats!"), //
    // stats logging
    START_STATS_LOGGING("!start_logstats!"), //
    STOP_STATS_LOGGING("!stop_logstats!"), // 
    // app control command
    APP_CONTROL("!app_control!");

    String value;


    EnumCommand(String s)
    {
        this.value = s;
    }


    public static EnumCommand find(String s)
    {
        for (EnumCommand c : EnumCommand.values())
        {
            if (c.value.equals(s))
                return c;
        }

        return null;
    }


    public String value()
    {
        return value;
    }


    public String toString()
    {
        return value;
    }

}
