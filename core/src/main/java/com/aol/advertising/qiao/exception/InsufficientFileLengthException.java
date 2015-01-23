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
 * File Name:   InsufficientFileLengthException.java	
 * Description:
 * @author:     ytung
  *
 ****************************************************************************/

package com.aol.advertising.qiao.exception;

public class InsufficientFileLengthException extends RuntimeException
{
    private static final long serialVersionUID = -3446809453414208341L;


    public InsufficientFileLengthException(String message, Throwable cause)
    {
        super(message, cause);
    }


    public InsufficientFileLengthException(String message)
    {
        super(message);
    }


    public InsufficientFileLengthException(Throwable cause)
    {
        super(cause);
    }

}
