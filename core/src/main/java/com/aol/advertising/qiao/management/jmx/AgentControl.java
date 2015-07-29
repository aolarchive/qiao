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

public class AgentControl extends ControlCommand
{
    private static final long serialVersionUID = -3161717308398411329L;


    public AgentControl(Object source, EnumCommand cmd)
    {
        super(source, cmd);
    }


    public AgentControl(Object source, EnumCommand cmd, String payload)
    {
        super(source, cmd, payload);
    }
}
