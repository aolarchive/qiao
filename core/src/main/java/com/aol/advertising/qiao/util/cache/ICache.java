/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2011-2013 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   ICache.java	
 * Description:
 * @author:     ytung
 * @version:    2.5
 *
 ****************************************************************************/

package com.aol.advertising.qiao.util.cache;

public interface ICache<K, V>
{
    public V get(K key);


    public V getAndTouch(K key);


    public V set(K key, V val, int expirationTime);


    public V delete(K key);


    public void clear();


    public boolean contain(K key);


    public V putIfAbsent(K key, V value, int expirationTime);


    public V replace(K key, V value, int expirationTime);


    public int size();

}
