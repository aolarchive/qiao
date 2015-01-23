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
import org.springframework.context.ApplicationContext;

import com.aol.advertising.qiao.agent.QiaoAgent;
import com.aol.advertising.qiao.bootstrap.Bootstrap;
import com.aol.advertising.qiao.config.ConfigConstants;
import com.aol.advertising.qiao.util.CommonUtils;

public class BootstrapTest
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
                "/jp1/qiao/src/test/resources/bootstrap");
        Bootstrap.main(null);

        CommonUtils.sleepQuietly(10);
        
       /*
        boolean to_continue = true;
        if (to_continue)
        {
            ApplicationContext ctx = Bootstrap.getAppCtx();
            QiaoAgent agent = ctx.getBean(QiaoAgent.class);

            CommonUtils.sleepQuietly(1);

            //agent.suspend();

            while (true)
                CommonUtils.sleepQuietly(10);               
        }
       */
    }

}
