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
 * File Name:   IEvictionListener.java	
 * Description:
 * @author:     ytung
 * @version:    2.0.1
 *
 ****************************************************************************/
 
package com.aol.advertising.qiao.util.cache;
 

public interface IEvictionListener<K, V>
{
    void onEviction(K key, V value);
}