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
 * File Name:   QiaoOperationException.java	
 * Description:
 * @author:     ytung
  *
 ****************************************************************************/

package com.aol.advertising.qiao.exception;

public class QiaoOperationException extends RuntimeException
{
    private static final long serialVersionUID = -3446809453414208341L;


    public QiaoOperationException(String message, Throwable cause)
    {
        super(message, cause);
    }


    public QiaoOperationException(String message)
    {
        super(message);
    }


    public QiaoOperationException(Throwable cause)
    {
        super(cause);
    }

}
