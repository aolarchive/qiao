/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2014 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   QuarantineException.java	
 * Description:
 * @author:     ytung
  *
 ****************************************************************************/

package com.aol.advertising.qiao.exception;

public class QuarantineException extends RuntimeException
{
    private static final long serialVersionUID = -3446809453414208341L;


    public QuarantineException(String message, Throwable cause)
    {
        super(message, cause);
    }


    public QuarantineException(String message)
    {
        super(message);
    }


    public QuarantineException(Throwable cause)
    {
        super(cause);
    }

}
