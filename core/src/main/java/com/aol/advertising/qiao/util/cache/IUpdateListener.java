/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2012-2013 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   IUpdateListener.java	
 * Description:
 * @author:     ytung
 * @version:    2.5
 *
 ****************************************************************************/

package com.aol.advertising.qiao.util.cache;

public interface IUpdateListener<K, V>
{
    void onAdd(K key, V value);
    void onUpdate(K key, V value);
    void onDelete(K key);
}
