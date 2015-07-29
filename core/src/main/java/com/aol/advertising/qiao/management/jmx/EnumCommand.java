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
