/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2013 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   BootstrapTest.java	
 * Description:
 * @author:     ytung05
 *
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
