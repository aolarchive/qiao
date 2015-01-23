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
 * File Name:   ITailer.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector.file;

/**
 * Interface implemented by tail injector's helper class.
 * 
 * @param <T>
 */
public interface ITailer<T> extends Runnable, IReader<T>
{
    public static enum TAIL_MODE
    {
        TEXT, TEXTBLOCK, BINARY, AVRO
    }
}
