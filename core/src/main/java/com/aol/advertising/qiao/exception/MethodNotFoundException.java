/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2011-2012 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   MethodNotFoundException.java	
 * Description:
 * @author:     ytung
 * @version:    2.0
 *
 ****************************************************************************/

package com.aol.advertising.qiao.exception;

public class MethodNotFoundException extends RuntimeException
{
    private static final long serialVersionUID = -3446809453414208341L;


    public MethodNotFoundException(String message, Throwable cause)
    {
        super(message, cause);
    }


    public MethodNotFoundException(String message)
    {
        super(message);
    }


    public MethodNotFoundException(Throwable cause)
    {
        super(cause);
    }

}
