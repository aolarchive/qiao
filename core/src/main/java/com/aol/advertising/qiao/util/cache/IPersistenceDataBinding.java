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
 * File Name:   IPersistenceDataBinding.java	
 * Description:
 * @author:     ytung
 * @version:    2.5
 *
 ****************************************************************************/
 
package com.aol.advertising.qiao.util.cache;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;


public interface IPersistenceDataBinding<K, V>
{
    public EntryBinding<K> getKeyBinding(StoredClassCatalog clzCatalog);
    public EntryBinding<V> getValueBinding(StoredClassCatalog clzCatalog);

}
