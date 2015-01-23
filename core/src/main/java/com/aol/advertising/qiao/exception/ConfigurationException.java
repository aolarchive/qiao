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
 * File Name:   ConfigurationException.java	
 * Description:
 * @author:     ytung
  *
 ****************************************************************************/

package com.aol.advertising.qiao.exception;

public class ConfigurationException extends RuntimeException
{
    private static final long serialVersionUID = -3446809453414208341L;


    public ConfigurationException(String message, Throwable cause)
    {
        super(message, cause);
    }


    public ConfigurationException(String message)
    {
        super(message);
    }


    public ConfigurationException(Throwable cause)
    {
        super(cause);
    }

}
