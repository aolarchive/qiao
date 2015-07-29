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
import java.util.concurrent.locks.Lock;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class ValueWrapper<V> implements Externalizable
{
    private static final long serialVersionUID = -1703117639197898259L;
    private static final byte TRUE = 0x01;
    private static final byte FALSE = 0x00;

    protected V value;

    protected long insertionTime;
    protected long lastAccessTime;

    protected int timeout; // when the value can be reaped (in ms)

    protected byte isModified = FALSE;

    protected transient Lock lock;


    /**
     * Default constructor - for marshalling
     */
    public ValueWrapper()
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
    public ValueWrapper(V value, int timeoutSecs)
    {
        this.value = value;
        this.timeout = timeoutSecs * 1000;
        this.insertionTime = System.currentTimeMillis();
        this.lastAccessTime = insertionTime;
        this.isModified = TRUE;
    }


    public ValueWrapper(V value, int timeoutSecs, boolean modFlag)
    {
        this.value = value;
        this.timeout = timeoutSecs * 1000;
        this.insertionTime = System.currentTimeMillis();
        this.lastAccessTime = insertionTime;
        this.isModified = (modFlag ? TRUE : FALSE);
    }


    /**
     * Returns the value object.
     *
     * @return the value object
     */
    public V getValue()
    {
        return value;
    }


    /**
     * Number of milliseconds from the last access time before this object can
     * be reaped.
     *
     * @return the expiration time in milliseconds
     */
    public int getTimeout()
    {
        return timeout;
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
        return value;
    }


    /**
     * Returns the timestamp this object was last accessed.
     *
     * @return the last access timestamp
     */
    public long getLastAccessTime()
    {
        return lastAccessTime;
    }


    /**
     * check if this object has been modified.
     *
     * @return true if this object has been modified.
     */
    public boolean isModified()
    {
        return isModified == TRUE;
    }


    /**
     * Set modification flag to the given value.
     *
     * @param isModified
     */
    public void setModified(boolean isModified)
    {
        this.isModified = isModified ? TRUE : FALSE;
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


    public Lock getLock()
    {
        return lock;
    }


    public void setLock(Lock lock)
    {
        this.lock = lock;
    }


    public long getInsertionTime()
    {
        return insertionTime;
    }


    public void setValue(V value)
    {
        this.value = value;
    }


    public void setInsertionTime(long insertionTime)
    {
        this.insertionTime = insertionTime;
    }


    public void setLastAccessTime(long lastAccessTime)
    {
        this.lastAccessTime = lastAccessTime;
    }


    public void setTimeout(int timeout)
    {
        this.timeout = timeout;
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException
    {
        this.insertionTime = in.readLong();
        this.lastAccessTime = in.readLong();
        this.timeout = in.readInt();
        this.isModified = in.readByte();
        this.value = (V) in.readObject();
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeLong(this.insertionTime);
        out.writeLong(this.lastAccessTime);
        out.writeInt(this.timeout);
        out.writeByte(this.isModified);
        out.writeObject(value);
    }


    @Override
    public boolean equals(Object obj)
    {
        ValueWrapper<V> that = (ValueWrapper<V>) obj;
        return value.equals(that.value);
    }

}
