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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.util.CommonUtils;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.serial.TupleSerialKeyCreator;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.collections.StoredEntrySet;
import com.sleepycat.collections.StoredIterator;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredSortedEntrySet;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

/**
 * A persistent map which supports the java Map interface for key/value pairs
 * with additional expiration support. The data in the map are stored on disk
 * and accessed from disk directly without caching. A reaper exists that
 * periodically evicts expired entries. In addition, if the map is configured to
 * be bounded, entries in excess of the max size can be evicted by the reaper
 * based on LRU (Least-Recently-Used) policy.
 *
 */
@ManagedResource(description = "Local Persistent Cache Store")
public class PersistentMap<K, W extends PersistentValueWrapper<K, ? >> extends
        StoredMap<K, W>
{

    interface MethodWrapper<K, T>
    {
        public T execute();
    }

    private static final Logger logger = Logger.getLogger(PersistentMap.class);

    private static final String SECOND_INDEX_NAME = "_Index_Expiry";
    private static final String THIRD_INDEX_NAME = "_Index_AccessTime";

    private AtomicInteger itemCount; // stored size to avoid expensive call to StoredMap.size()

    private ScheduledThreadPoolExecutor timer;
    private Future< ? > task = null;
    private int reapingIntervalSecs = 60;
    private int reapingInitDelaySecs = 10;
    private boolean refreshExpiryOnAccess = false;
    //
    private long lastEvictTime;
    // private boolean enableEvictOnSet = false; // disable by default
    private AtomicBoolean evictInProgress = new AtomicBoolean(false);
    //
    private int highWaterMark = 0;
    private int lowWaterMark = 0;
    //
    private Set<IEvictionListener<K, W>> evictListeners = new HashSet<IEvictionListener<K, W>>();
    private Set<IUpdateListener<K, W>> cacheListeners = new HashSet<IUpdateListener<K, W>>();
    //

    private Database database;
    private SecondaryDatabase expiryIndexDB;
    private SecondaryDatabase accessTimeIndexDB;
    private String databaseName;
    private Environment dbEnvironment;
    private DatabaseConfig dbConfig;
    private StoredClassCatalog javaClassCatalog;
    private CacheMode cacheMode;

    private boolean isTransactional = true;
    private String jeProperties = "persistence.properties";

    private StoredSortedMap<Long, W> dataByExpiryMap;
    private StoredSortedMap<Long, W> dataByAccessTimeMap;
    private EntryBinding<W> valueBinding;
    private String id = "";

    private CurrentTransaction currTrans;

    private int lockConflictRetryMsecs = 100;
    private int maxTries = 4;


    /**
     * @param database
     * @param javaClassCatalog
     * @param dataBinding
     * @param writeAllowed
     * @param highWaterMark
     * @param lowWaterMark
     * @param reapingIntervalSecs
     * @throws Exception
     */
    public PersistentMap(Database database,
            StoredClassCatalog javaClassCatalog,
            IPersistenceDataBinding<K, W> dataBinding, boolean writeAllowed,
            int highWaterMark, int lowWaterMark, int reapingInitDelaySecs,
            int reapingIntervalSecs) throws Exception
    {
        super(database, dataBinding.getKeyBinding(javaClassCatalog),
                dataBinding.getValueBinding(javaClassCatalog), writeAllowed);

        this.database = database;
        this.javaClassCatalog = javaClassCatalog;
        this.valueBinding = dataBinding.getValueBinding(javaClassCatalog);
        this.highWaterMark = highWaterMark;
        this.lowWaterMark = lowWaterMark;
        this.reapingInitDelaySecs = reapingInitDelaySecs;
        this.reapingIntervalSecs = reapingIntervalSecs;

    }


    public PersistentMap(Database database,
            StoredClassCatalog javaClassCatalog, EntryBinding<K> keyBinding,
            EntryBinding<W> valueBinding, boolean writeAllowed,
            int highWaterMark, int lowWaterMark, int reapingInitDelaySecs,
            int reapingIntervalSecs) throws Exception
    {
        super(database, keyBinding, valueBinding, writeAllowed);

        this.database = database;
        this.javaClassCatalog = javaClassCatalog;
        this.valueBinding = valueBinding;
        this.highWaterMark = highWaterMark;
        this.lowWaterMark = lowWaterMark;
        this.reapingInitDelaySecs = reapingInitDelaySecs;
        this.reapingIntervalSecs = reapingIntervalSecs;
    }


    public void init() throws Exception
    {
        this.dbConfig = database.getConfig();
        this.dbEnvironment = database.getEnvironment();
        this.databaseName = database.getDatabaseName();
        this.isTransactional = dbConfig.getTransactional();
        this.cacheMode = dbConfig.getCacheMode();

        if (highWaterMark > 0 && lowWaterMark == 0)
            this.lowWaterMark = (int) Math.round(0.9 * highWaterMark);

        this.itemCount = new AtomicInteger(super.size()); // init count

        _openSecondaryIndexes();

        if (timer == null)
            timer = new ScheduledThreadPoolExecutor(1);

        enableReaping(reapingInitDelaySecs, reapingIntervalSecs);

    }


    public void close()
    {
        if (task != null)
        {
            task.cancel(true); // may interrupt
            task = null;
        }

        if (timer != null)
        {
            CommonUtils.shutdownAndAwaitTermination(timer); // TODO: check if wait time is long enough
            timer = null;
        }

        if (expiryIndexDB != null)
            expiryIndexDB.close();
        if (accessTimeIndexDB != null)
            accessTimeIndexDB.close();

        logger.info("persistent map closed");

        // Note: database, javaClassCatalog, and dbEnvironment must be closed by external caller
        // since they are not opened by this class.
    }


    private void _openSecondaryIndexes() throws Exception
    {
        _openDataByExpiryDB(javaClassCatalog);
        _openDataByAccessTimeDB(javaClassCatalog);
    }


    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void _openDataByExpiryDB(StoredClassCatalog javaClassCatalog)
    {
        // ----- Index by expiration time
        SecondaryConfig sec_config = new SecondaryConfig();
        sec_config.setTransactional(isTransactional);
        sec_config.setAllowCreate(true);
        sec_config.setCacheMode(cacheMode);
        sec_config.setSortedDuplicates(true);

        sec_config
                .setKeyCreator(new TupleSerialKeyCreator<PersistentValueWrapper>(
                        javaClassCatalog, PersistentValueWrapper.class)
                {

                    @Override
                    public boolean createSecondaryKey(
                            TupleInput primaryKeyInput,
                            PersistentValueWrapper dataInput,
                            TupleOutput indexKeyOutput)
                    {
                        indexKeyOutput.writeLong(dataInput.getExpirationTime());
                        return true;
                    }

                });

        expiryIndexDB = dbEnvironment.openSecondaryDatabase(null, databaseName
                + SECOND_INDEX_NAME, database, sec_config);

        dataByExpiryMap = new StoredSortedMap(expiryIndexDB,
                TupleBinding.getPrimitiveBinding(Long.class), valueBinding,
                true);

    }


    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void _openDataByAccessTimeDB(StoredClassCatalog javaClassCatalog)
    {
        // ----- Index by last access time
        SecondaryConfig third_config = new SecondaryConfig();
        third_config.setTransactional(isTransactional);
        third_config.setAllowCreate(true);
        third_config.setCacheMode(cacheMode);
        third_config.setSortedDuplicates(true);

        third_config
                .setKeyCreator(new TupleSerialKeyCreator<PersistentValueWrapper>(
                        javaClassCatalog, PersistentValueWrapper.class)
                {

                    @Override
                    public boolean createSecondaryKey(
                            TupleInput primaryKeyInput,
                            PersistentValueWrapper dataInput,
                            TupleOutput indexKeyOutput)
                    {
                        indexKeyOutput.writeLong(dataInput.getLastAccessTime());
                        return true;
                    }

                });

        accessTimeIndexDB = dbEnvironment.openSecondaryDatabase(null,
                databaseName + THIRD_INDEX_NAME, database, third_config);

        dataByAccessTimeMap = new StoredSortedMap(accessTimeIndexDB,
                TupleBinding.getPrimitiveBinding(Long.class), valueBinding,
                true);

    }


    protected W invoke(MethodWrapper<K, W> method)
    {
        for (int i = 0; i < maxTries;)
        {
            try
            {
                return method.execute();

            }
            catch (LockConflictException e)
            {
                if (i++ < maxTries)
                {
                    CommonUtils.sleepQuietly(lockConflictRetryMsecs);
                    continue;
                }
                else
                    throw e;
            }
        }

        return null;
    }


    private W _putIfAbsent(K key, W value)
    {
        value.setModified(true);
        W retval = super.putIfAbsent(key, value);
        if (retval != null)
            return retval; // no change

        itemCount.incrementAndGet();
        notifyCacheListenersOnChange(key, value);
        return null;

    }


    @Override
    public W putIfAbsent(final K key, final W value)
    {
        W res = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {
                return _putIfAbsent(key, value);
            }

        });

        return res;
    }


    private W _replace(K key, W wrapper)
    {
        W w = super.replace(key, wrapper);
        notifyCacheListenersOnChange(key, wrapper);
        return w;
    }


    @Override
    public W replace(final K key, final W wrapper)
    {

        W res = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {

                return _replace(key, wrapper);

            }

        });

        return res;
    }


    @SuppressWarnings("unchecked")
    private W _get(Object key)
    {
        if (logger.isTraceEnabled())
            logger.trace("get(" + key + ")");

        W val = super.get(key);
        if (val == null)
            return null;

        if (isExpired(val))
        {
            logger.trace(key + " expired");
            itemCount.decrementAndGet();
            notifyCacheListenersOnDelete((K) key);
            return null;
        }

        notifyCacheListenersOnAdd((K) key, val); // add to cache if absent

        if (refreshExpiryOnAccess)
            return touch((K) key, val);
        else
            return val;
    }


    /**
     * Returns the value to which the specified key is mapped in the cache, or
     * null if this cache contains no mapping for the key.
     *
     * @param key
     * @return the mapped value of the given key
     */
    @Override
    public W get(final Object key)
    {
        W res = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {

                return _get(key);
            }

        });

        return res;

    }


    private W _get(K key, boolean refreshExpiry)
    {
        if (logger.isTraceEnabled())
            logger.trace("get(" + key + ")");

        W val = super.get(key);
        if (val == null)
            return null;

        if (isExpired(val))
        {
            itemCount.decrementAndGet();
            notifyCacheListenersOnDelete(key);
            return null;
        }

        notifyCacheListenersOnAdd(key, val); // add to cache if absent

        if (refreshExpiry)
            return touch(key, val);
        else
            return val;
    }


    /**
     * Returns the value to which the specified key is mapped in the cache, or
     * null if this cache contains no mapping for the key.
     *
     * @param key
     * @param refreshExpiry
     *            true to reset the entry's expiry time
     * @return
     */
    public W get(final K key, final boolean refreshExpiry)
    {
        W res = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {

                return _get(key, refreshExpiry);

            }

        });

        return res;

    }


    private W _getAndTouch(K key, int expirationTime)
    {
        if (logger.isTraceEnabled())
            logger.trace("getAndTouch(" + key + "," + expirationTime + ")");

        W val = super.get(key);
        if (val == null)
            return null;

        if (isExpired(val))
        {
            itemCount.decrementAndGet();
            notifyCacheListenersOnDelete(key);
            return null;
        }

        notifyCacheListenersOnAdd(key, val); // add to cache if absent

        return touch(key, val, expirationTime);
    }


    /**
     * Get the value associated with the given key and reset the entry's expiry
     * to the specific expiration time. Returns null if this cache contains no
     * mapping for such a key.
     *
     * @param key
     * @param expirationTime
     *            the new expiration value
     * @return the value
     */
    public W getAndTouch(final K key, final int expirationTime)
    {
        W res = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {
                return _getAndTouch(key, expirationTime);
            }

        });

        return res;
    }


    private W touch(K key, W val, int expirationTime)
    {
        val.touch(expirationTime);
        super.replace(key, val);
        return val;
    }


    private W touch(K key, W val)
    {
        val.touch();
        super.replace(key, val);
        return val;
    }


    private W _getAndTouch(K key)
    {
        if (logger.isTraceEnabled())
            logger.trace("getAndTouch(" + key + ")");

        W val = super.get(key);
        if (val == null)
            return null;

        if (isExpired(val))
        {
            itemCount.decrementAndGet();
            notifyCacheListenersOnDelete(key);
            return null;
        }

        notifyCacheListenersOnAdd(key, val); // add to cache if absent

        return touch(key, val);
    }


    /**
     * Returns the value associated with the given key and reset the entry's
     * expiration time regardless of the setting of refreshExpiryOnAccess.
     * Returns null if this cache contains no mapping for such a key.
     *
     * @param key
     * @return the mapped value of the given key
     */
    public W getAndTouch(final K key)
    {
        W res = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {
                return _getAndTouch(key);
            }

        });

        return res;

    }


    private void _setModifiedFlag(K key, boolean flag)
    {
        if (logger.isTraceEnabled())
            logger.trace("setModifiedFlag(" + key + "," + flag + ")");

        W val = super.get(key);
        if (val != null)
        {
            if (val.isModified() != flag)
            {
                val.setModified(flag);
                super.replace(key, val);
            }
        }
    }


    /**
     * Explicitly set modified flag of entry with the given key.
     *
     * @param key
     */
    public void setModifiedFlag(final K key, final boolean flag)
    {
        invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {
                _setModifiedFlag(key, flag);
                return null;
            }

        });

    }


    private void _clear()
    {
        super.clear();
        itemCount.set(0);
    }


    /**
     * Remove all the entries from the cache. Be careful with this!
     */
    @Override
    public void clear()
    {
        invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {
                _clear();
                return null;
            }

        });

    }


    private W _getEntry(K key)
    {
        if (logger.isTraceEnabled())
            logger.trace("getEntry(" + key + ")");

        return super.get(key);
    }


    /**
     * Returns the wrapper object mapped to the given key. This is for internal
     * use. It does not touch the last access time. In addition, it does not
     * notify cache listeners.
     *
     * @param key
     * @return the wrapper object
     */
    public W getEntry(final K key)
    {
        W res = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {
                return _getEntry(key);
            }

        });

        return res;
    }


    /**
     * Adds a listener for the eviction process.
     *
     * @param l
     */
    public void addEvictionListener(IEvictionListener<K, W> l)
    {
        evictListeners.add(l);
    }


    /**
     * Adds a listener for the update process.
     *
     * @param l
     */
    public void addUpdateListener(IUpdateListener<K, W> l)
    {
        cacheListeners.add(l);
    }


    /**
     * Removes the listener from the eviction process.
     *
     * @param l
     */
    public void removeEvictionListener(IEvictionListener<K, W> l)
    {
        evictListeners.remove(l);
    }


    /**
     * Removes the listener from the update process.
     *
     * @param l
     */
    public void removeUpdateListener(IUpdateListener<K, W> l)
    {
        cacheListeners.remove(l);
    }


    private int _getDiskItemCount()
    {
        return super.size();
    }


    /**
     * Returns the item count of the map currently on the disk. It may include
     * expired items which have not been removed yet until next cleanup cycle.
     * This operation is considered expensive.
     *
     * @return returns the item count on the disk belonging to this map.
     */
    @ManagedAttribute
    public int getDiskItemCount()
    {
        for (int i = 0; i < maxTries;)
        {
            try
            {
                return _getDiskItemCount();

            }
            catch (LockConflictException e)
            {
                if (i++ < maxTries)
                {
                    CommonUtils.sleepQuietly(lockConflictRetryMsecs);
                    continue;
                }
                else
                    throw e;
            }
        }

        return -1;

    }


    /**
     * Returns true if reaping process is enabled for removing expiry entries.
     *
     * @return true if reaping process is enabled for removing expiry entries.
     */
    @ManagedAttribute
    public boolean isReapingEnabled()
    {
        return task != null && !task.isCancelled();
    }


    /**
     * Runs the reaper every interval seconds, evicts expired items
     *
     * @param interval
     *            number of seconds
     */
    @ManagedOperation
    public void enableReaping(int delay, int interval)
    {
        if (isReapingEnabled())
            return;

        this.reapingInitDelaySecs = delay;
        this.reapingIntervalSecs = interval;

        if (task != null)
            task.cancel(false);

        lastEvictTime = System.currentTimeMillis();
        task = timer.scheduleWithFixedDelay(new ExpirationReaper(), delay,
                interval, TimeUnit.SECONDS);
    }


    /**
     * Disables the reaper.
     */
    @ManagedOperation
    public void disableReaping()
    {
        if (task != null)
        {
            task.cancel(false);
            task = null;
        }
    }


    protected void evict()
    {
        if (evictInProgress.compareAndSet(false, true))
        {
            logger.trace("<evict starts>");

            lastEvictTime = System.currentTimeMillis();

            for (int i = 0; i < maxTries;)
            {
                if (i > 0)
                    logger.info("Retry " + i
                            + " on evicting expiry entries ...");
                try
                {
                    evictExpiry(); // only evict expired items
                    break;
                }
                catch (LockConflictException ex)
                {
                    if (i++ < maxTries)
                    {
                        CommonUtils.sleepQuietly(lockConflictRetryMsecs);
                        continue;
                    }
                    else
                        logger.warn("LockTimeoutException during evictExpiry: "
                                + ex.getMessage());
                }
            }

            if (highWaterMark > 0 && (itemCount.get() > highWaterMark))
            {
                for (int i = 0; i < maxTries;)
                {
                    if (i > 0)
                        logger.info("Retry " + i
                                + " on evicting oldest entries ...");
                    try
                    {
                        evictOldest();
                        break;
                    }
                    catch (LockConflictException ex)
                    {
                        if (i++ < maxTries)
                        {
                            CommonUtils.sleepQuietly(lockConflictRetryMsecs);
                            continue;
                        }
                        else
                            logger.warn("LockTimeoutException during evitOldest: "
                                    + ex.getMessage());
                    }
                }
            }

            getAndSetEntryCount();

            logger.trace("<evict ends> size=" + itemCount.get());

            evictInProgress.set(false);
        }
        else
            logger.trace("Evict operation already in progress. Skip.");

    }


    protected void evictExpiry() throws LockTimeoutException
    {
        boolean success = false;
        int n = 0;
        StoredIterator<Map.Entry<Long, W>> iter = null;
        try
        {
            beginTransaction(); // must be transactional
            @SuppressWarnings("unchecked")
            StoredSortedEntrySet<Long, W> ses = (StoredSortedEntrySet<Long, W>) dataByExpiryMap
                    .entrySet();
            iter = ses.storedIterator(true);
            for (; iter.hasNext();)
            {
                Map.Entry<Long, W> entry = iter.next();
                W val = entry.getValue();
                logger.trace("checking " + val.key + ",  expiry="
                        + val.expirationTime);
                if (val != null)
                {
                    if (isExpired(val))
                    {
                        if (logger.isTraceEnabled())
                            logger.trace(">> evicting expired " + val.getKey()
                                    + ": " + val.getValue());
                        iter.remove();
                        logger.trace(">> " + val.getKey() + " removed");
                        itemCount.decrementAndGet();
                        notifyEvictionListeners(val.getKey(), val);
                        ++n;
                    }
                    else
                        break;
                }
            }
            success = true;
        }
        catch (LockConflictException ex)
        {
            throw ex;
        }
        catch (Exception ex)
        {
            logger.warn(ex.getMessage(), ex);
        }
        finally
        {

            if (!success)
            {
                abortTransaction();
            }
            else
            {
                if (iter != null)
                    iter.close();

                // must follow iter.close()
                commitTransaction();
            }

        }

        if (n > 0)
            logger.info(id + ": expiration evict count: " + n);

    }


    protected void evictOldest() throws LockTimeoutException
    {
        StoredIterator<Map.Entry<Long, W>> iter = null;

        boolean success = false;
        try
        {
            beginTransaction();
            if (highWaterMark > 0 && (itemCount.get() > highWaterMark))
            {
                // still too many entries: now evict entries based on insertion time: oldest first
                int diff = itemCount.get() - lowWaterMark; // we have to evict diff entries
                logger.trace("need to evict " + diff + " oldest entries");

                @SuppressWarnings("unchecked")
                StoredSortedEntrySet<Long, W> ses = (StoredSortedEntrySet<Long, W>) dataByAccessTimeMap
                        .entrySet();
                iter = ses.storedIterator(true);
                for (; iter.hasNext() && (diff-- > 0);)
                {

                    Map.Entry<Long, W> entry = iter.next();
                    W val = entry.getValue();
                    iter.remove();
                    K k = val.key;
                    if (logger.isTraceEnabled())
                        logger.trace("evicting oldest " + k + ": "
                                + entry.getKey());
                    notifyEvictionListeners(k, val);
                }

                success = true;
            }
        }
        catch (LockTimeoutException ex)
        {
            throw ex;
        }
        catch (Exception ex)
        {
            logger.warn(ex.getMessage(), ex);
        }
        finally
        {
            if (!success)
            {
                abortTransaction();
            }
            else
            {

                if (iter != null)
                {
                    iter.close();
                    logger.trace("(oldest iterator closed)");
                }

                commitTransaction();
            }
        }

    }


    private W _lookup(String key)
    {
        return super.get(key);
    }


    /**
     * Looks up the value of the given key. This operation does not reset the
     * expiry time.
     *
     * @param key
     * @return the value mapped to the given key, or null if not found.
     */
    @ManagedOperation(description = "Get the specific key's value from the cache")
    @ManagedOperationParameters({ @ManagedOperationParameter(name = "key", description = "The key value of the cached item") })
    public String lookup(final String key)
    {
        W val = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {
                return _lookup(key);
            }

        });

        if (val == null)
            return null;

        return val.value.toString();
    }


    protected boolean isExpired(W val)
    {
        return (val.timeout > 0 && System.currentTimeMillis() > (val.expirationTime));
    }


    @ManagedAttribute
    public int getReapingIntervalSecs()
    {
        return reapingIntervalSecs;
    }


    public void setReapingIntervalSecs(int reapingIntervalSecs)
    {
        this.reapingIntervalSecs = reapingIntervalSecs;
    }


    /**
     * @return the contents of the map. <br>
     *         Note: This method does not retry when lock times out.
     */
    public String dump()
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<K, W> entry : super.entrySet())
        {
            sb.append(entry.getKey()).append(": ");
            Object val = entry.getValue().getValue();
            if (val != null)
            {
                if (val instanceof byte[])
                    sb.append(" (" + ((byte[]) val).length).append(" bytes)");
                else
                    sb.append(val);
            }
            sb.append("\n");
        }
        return sb.toString();
    }


    protected void notifyEvictionListeners(K key, W value)
    {
        for (IEvictionListener<K, W> lstnr : evictListeners)
        {
            try
            {
                lstnr.onEviction(key, value);
            }
            catch (Exception t)
            {
                logger.error("failed notifying eviction listener", t);
            }
        }
    }


    protected void notifyCacheListenersOnAdd(K key, W value)
    {
        for (IUpdateListener<K, W> lstnr : cacheListeners)
        {
            try
            {
                lstnr.onAdd(key, value);
            }
            catch (Exception t)
            {
                logger.error("failed notifying update listener", t);
            }
        }
    }


    protected void notifyCacheListenersOnChange(K key, W value)
    {
        for (IUpdateListener<K, W> lstnr : cacheListeners)
        {
            try
            {
                lstnr.onUpdate(key, value);
            }
            catch (Exception t)
            {
                logger.error("failed notifying update listener", t);
            }
        }
    }


    protected void notifyCacheListenersOnDelete(K key)
    {
        for (IUpdateListener<K, W> lstnr : cacheListeners)
        {
            try
            {
                lstnr.onDelete(key);
            }
            catch (Exception t)
            {
                logger.error("failed notifying update listener", t);
            }
        }
    }

    protected class ExpirationReaper implements Runnable
    {
        public void run()
        {
            evict();
        }
    }


    @ManagedAttribute
    public boolean isRefreshExpiryOnAccess()
    {
        return refreshExpiryOnAccess;
    }


    public void setRefreshExpiryOnAccess(boolean refreshExpiryOnAccess)
    {
        this.refreshExpiryOnAccess = refreshExpiryOnAccess;
    }


    /**
     * Returns the high water mark of the cache. The system will start ejecting
     * values out of memory when this watermark is met. Ejected values need to
     * be fetched from disk, when accessed. A value of 0 means it is unbounded.
     *
     * @return the high water mark value
     */
    @ManagedAttribute
    public int getHighWaterMark()
    {
        return highWaterMark;
    }


    /**
     * Sets the high water mark of the cache. When this value is exceeded we
     * evict older entries, until we drop below this mark again. This
     * effectively maintains a bounded cache. A value of 0 means don't bound the
     * cache.
     * */
    public void setHighWaterMark(int highWatermark)
    {
        this.highWaterMark = highWatermark;
    }


    /**
     * Returns the low water mark of this cache. The system does not do anything
     * when this watermark is reached but this is the 'goal' of the system when
     * it starts ejecting data as a result of high watermark being met.
     *
     * @return the low water mark of this cache
     */
    @ManagedAttribute
    public int getLowWaterMark()
    {
        return lowWaterMark;
    }


    /**
     * Sets the low water mark of this cache. The system does not do anything
     * when this watermark is reached but this is the 'goal' of the system when
     * it starts ejecting data as a result of high watermark being met. If set
     * to 0, the system will set low water mark to 90% value of high water mark.
     *
     * @param lowWatermark
     */
    public void setLowWaterMark(int lowWatermark)
    {
        this.lowWaterMark = lowWatermark;
    }


    @ManagedAttribute
    public String getId()
    {
        return id;
    }


    public void setId(String id)
    {
        this.id = id;
    }


    @Override
    public boolean containsKey(Object key)
    {
        for (int i = 0; i < maxTries;)
        {
            try
            {
                return super.containsKey(key);
            }
            catch (LockConflictException e)
            {
                if (i++ < maxTries)
                {
                    CommonUtils.sleepQuietly(lockConflictRetryMsecs);
                    continue;
                }
                else
                    throw e;
            }
        }

        return false;
    }


    public void setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
    }


    private int getAndSetEntryCount()
    {
        itemCount.set(super.size());
        return itemCount.get();
    }


    @Override
    public Set<Entry<K, W>> entrySet()
    {
        for (int i = 0; i < maxTries;)
        {
            try
            {
                return super.entrySet();
            }
            catch (LockConflictException e)
            {
                if (i++ < maxTries)
                {
                    CommonUtils.sleepQuietly(lockConflictRetryMsecs);
                    continue;
                }
                else
                    throw e;
            }
        }

        return null;
    }


    @SuppressWarnings("unchecked")
    public StoredIterator<Map.Entry<K, W>> iterator()
    {
        return ((StoredEntrySet<K, W>) super.entrySet()).storedIterator(true);
    }


    @SuppressWarnings("unchecked")
    public StoredIterator<Map.Entry<Long, W>> accessOrderIterator()
    {
        return ((StoredSortedEntrySet<Long, W>) dataByAccessTimeMap.entrySet())
                .storedIterator(true);
    }


    @SuppressWarnings("unchecked")
    public StoredIterator<Map.Entry<Long, W>> expiryOrderIterator()
    {
        return ((StoredSortedEntrySet<Long, W>) dataByExpiryMap.entrySet())
                .storedIterator(true);
    }


    /**
     * @return count of number of cache entries. This operation does not require
     *         walking the disk files.
     */
    public int getItemCount()
    {
        return itemCount.get();
    }


    @ManagedAttribute
    public long getLastEvictTime()
    {
        return lastEvictTime;
    }


    @ManagedAttribute
    public String getJePropertiesName()
    {
        return jeProperties;
    }


    public void setJeProperties(String jeProperties)
    {
        this.jeProperties = jeProperties;
    }


    @Deprecated
    @Override
    public K append(W value)
    {
        throw new UnsupportedOperationException("<append> is not supported");
    }


    @SuppressWarnings("unchecked")
    private W _remove(Object key)
    {
        if (logger.isTraceEnabled())
            logger.trace("remove(" + key + ")");

        W val = super.remove(key);
        if (val != null)
            itemCount.decrementAndGet();

        // make sure cache copy is removed
        notifyCacheListenersOnDelete((K) key);

        return val;
    }


    @Override
    public W remove(final Object key)
    {

        W res = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {
                return _remove(key);
            }

        });

        return res;
    }


    private W _put(K key, W value)
    {
        W old = super.put(key, value);
        if (old == null)
            itemCount.incrementAndGet();

        notifyCacheListenersOnChange(key, value);
        return old;
    }


    @Override
    public W put(final K key, final W value)
    {

        W res = invoke(new MethodWrapper<K, W>()
        {

            @Override
            public W execute()
            {
                return _put(key, value);
            }

        });

        return res;

    }


    @SuppressWarnings("unchecked")
    private boolean _remove(Object key, Object value)
    {
        boolean ok = super.remove(key, value);
        if (ok)
        {
            itemCount.decrementAndGet();
            notifyCacheListenersOnDelete((K) key);
        }

        return ok;
    }


    @Override
    public boolean remove(final Object key, final Object value)
    {
        for (int i = 0; i < maxTries;)
        {
            try
            {
                return _remove(key, value);
            }
            catch (LockConflictException e)
            {
                if (i++ < maxTries)
                {
                    CommonUtils.sleepQuietly(lockConflictRetryMsecs);
                    continue;
                }
                else
                    throw e;
            }
        }

        return false;
    }


    public int getReapingInitDelaySecs()
    {
        return reapingInitDelaySecs;
    }


    public void setReapingInitDelaySecs(int reapingInitDelaySecs)
    {
        this.reapingInitDelaySecs = reapingInitDelaySecs;
    }


    public ScheduledThreadPoolExecutor getTimer()
    {
        return timer;
    }


    /**
     * Starts a transaction. To commit the transaction, call
     * commitTransaction().
     *
     * @return the handle to the transaction, or null if unable to start a
     *         transaction.
     */
    public Transaction beginTransaction()
    {
        if (currTrans == null)
            currTrans = CurrentTransaction.getInstance(dbEnvironment);

        Transaction otrans = currTrans.getTransaction();
        if (otrans != null && otrans.isValid())
        {
            logger.warn("transaction is already active");
            return otrans;
        }

        Transaction trans = currTrans.beginTransaction(null);
        if (trans == null)
            logger.warn("unable to start a transaction");

        return trans;
    }


    /**
     * This method is deprecated. Use commitTransaction instead.
     *
     * @param transaction
     */
    @Deprecated
    public void commit(Transaction transaction)
    {
        for (int i = 0; i < maxTries;)
        {
            try
            {
                currTrans.commitTransaction();
                return;
            }
            catch (LockConflictException e)
            {
                if (i++ < maxTries)
                {
                    CommonUtils.sleepQuietly(lockConflictRetryMsecs);
                    continue;
                }
                else
                    throw e;
            }
        }

    }


    /**
     * Commit a transaction. Will try up to maxTries if lock times out. Note
     * that if there is no active transaction with current thread, the call will
     * simply return.
     */
    public void commitTransaction()
    {
        for (int i = 0; i < maxTries;)
        {
            if (i > 0)
                logger.debug(i + " retry commit");

            try
            {
                Transaction trans = currTrans.getTransaction();
                if (trans != null)
                    currTrans.commitTransaction();

                return;
            }
            catch (LockConflictException e)
            {
                if (i++ < maxTries)
                {
                    if (logger.isDebugEnabled())
                        logger.debug(e.getClass().getSimpleName() + ": "
                                + e.getMessage());

                    CommonUtils.sleepQuietly(lockConflictRetryMsecs);
                    continue;
                }
                else
                    throw e;
            }
        }

    }


    /**
     * Aborts the current active transaction.
     */
    public void abortTransaction()
    {
        if (currTrans != null)
        {
            Transaction trans = currTrans.getTransaction();
            if (trans != null)
                currTrans.abortTransaction();
            else
                logger.warn("No transaction is active to abort");
        }
    }


    public void setTimer(ScheduledThreadPoolExecutor timer)
    {
        this.timer = timer;
    }


    public void setLockConflictRetryMsecs(int lockConflictRetryMsecs)
    {
        this.lockConflictRetryMsecs = lockConflictRetryMsecs;
    }


    public int getMaxTries()
    {
        return maxTries;
    }


    public void setMaxTries(int maxTries)
    {
        this.maxTries = maxTries;
    }

}
