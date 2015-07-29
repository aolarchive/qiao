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

package com.aol.advertising.qiao;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aol.advertising.qiao.bootstrap.Bootstrap;
import com.aol.advertising.qiao.config.ConfigConstants;
import com.aol.advertising.qiao.util.CommonUtils;

public class AMQEmitterTest
{

    @Before
    public void setUp() throws Exception
    {
    }


    @After
    public void tearDown() throws Exception
    {
    }


    @Test
    public void testMain()
    {
        System.setProperty(ConfigConstants.PROP_QIAO_CFG_DIR,
                "/jp1/qiao/src/test/resources/amq");
        Bootstrap.main(null);


       while(true)
        CommonUtils.sleepQuietly(1);
    }

}
