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

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationEvent;

public abstract class ControlCommand extends ApplicationEvent
{
    private static final long serialVersionUID = 5006355869030809854L;
    protected static Logger logger = Logger.getLogger(ControlCommand.class);
    protected EnumCommand command;
    protected String payload;


    ControlCommand(Object source, EnumCommand cmd)
    {
        super(source);
        this.command = cmd;
    }


    ControlCommand(Object source, EnumCommand cmd, String payload)
    {
        super(source);
        this.command = cmd;
        if (payload != null && payload.length() > 0)
            this.payload = payload;
    }


    public EnumCommand getCommand()
    {
        return command;
    }


    public static ControlCommand createCommand(Object src, String cmdText,
            String payload)
    {
        ControlCommand cmd = null;

        EnumCommand c = EnumCommand.find(cmdText);
        if (c != null)
        {
            switch (c)
            {
                case START_AGENT:
                case SUSPEND_AGENT:
                case RESUME_AGENT:
                    cmd = new AgentControl(src, c);
                    break;

                case START_STATS:
                case STOP_STATS:
                case RESET_STATS:
                case START_STATS_LOGGING:
                case STOP_STATS_LOGGING:
                    cmd = new StatsControl(src, c);
                    break;

                case RESET_INBOUND_CONNECTION:
                case RESET_OUTBOUND_CONNECTION:
                    cmd = new ConnectionControl(src, c);
                    break;

                case APP_CONTROL:
                    cmd = new AppControl(src, c, payload);
                    break;

                default:
                    logger.error("Unsupported command: " + cmdText);
            }
        }
        else
        {
            logger.error("Invalid command: " + cmdText);
        }

        return cmd;

    }


    public String getPayload()
    {
        return payload;
    }


    public void setPayload(String payload)
    {
        this.payload = payload;
    }


    public String toString()
    {
        return command.value + (payload != null ? payload : "");
    }
}
