/****************************************************************************
 * Copyright (c) 2015 AOL Inc.
 * @author:     ytung05
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ****************************************************************************/

package com.aol.advertising.qiao.util.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class PersistentValueWrapper<K, V> extends ValueWrapper<V> implements
        Externalizable
{

    protected K key;
    protected long expirationTime;


    /**
     * Default constructor - for marshalling only
     */
    public PersistentValueWrapper()
    {
    }


    /**
     * Constructor.
     *
     * @param value
     *            the value object
     * @param timeoutSecs
     *            the expiration time in seconds for the given object
     */
    public PersistentValueWrapper(K key, V value, int timeoutSecs)
    {
        super(value, timeoutSecs);
        this.key = key;
        this.expirationTime = expirationTime();
    }


    public PersistentValueWrapper(K key, V value, int timeoutSecs,
            boolean modFlag)
    {
        super(value, timeoutSecs, modFlag);
        this.key = key;
        this.expirationTime = expirationTime();
    }


    private long expirationTime()
    {
        if (timeout > 0)
            return lastAccessTime + timeout;
        else
            return Long.MAX_VALUE;
    }


    /**
     * Reset the expiration time to the new value.
     *
     * @param timeoutSecs
     *            new expiration time in seconds
     * @return the value object
     */
    public V touch(int timeoutSecs)
    {
        lastAccessTime = System.currentTimeMillis();
        timeout = timeoutSecs * 1000;
        expirationTime = expirationTime();
        return value;
    }


    /**
     * Reset the expiration time.
     *
     * @return the value object
     */
    public V touch()
    {
        lastAccessTime = System.currentTimeMillis();
        expirationTime = expirationTime();
        return value;
    }


    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return ReflectionToStringBuilder.toString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }


    public void setLastAccessTime(long lastAccessTime)
    {
        this.lastAccessTime = lastAccessTime;
        this.expirationTime = expirationTime();

    }


    public void setTimeout(int timeout)
    {
        this.timeout = timeout;
        this.expirationTime = expirationTime();

    }


    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException
    {
        super.readExternal(in);
        this.expirationTime = in.readLong();
        this.key = (K) in.readObject();
        //System.out.println("<readExternal> " + toString()); //TODO: remove
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        //System.out.println("<writeExternal> " + this.toString()); //TODO: remove
        super.writeExternal(out);
        out.writeLong(this.expirationTime);
        out.writeObject(key);
    }


    public long getExpirationTime()
    {
        return expirationTime;
    }


    public void setExpirationTime(long expirationTime)
    {
        this.expirationTime = expirationTime;
    }


    public K getKey()
    {
        return key;
    }


    public void setKey(K key)
    {
        this.key = key;
    }


    @Override
    public boolean equals(Object obj)
    {
        PersistentValueWrapper<K, V> that = (PersistentValueWrapper<K, V>) obj;

        return super.equals(obj) && key.equals(that.key);
    }

}
