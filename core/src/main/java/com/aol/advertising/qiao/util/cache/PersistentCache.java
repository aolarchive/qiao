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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;

import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.util.CommonUtils;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredIterator;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;

/**
 * A persistent cache which supports operations for key/value pairs access with
 * additional expiration support. The data in the map are persisted on disk and
 * can be cached in memory based on access order. A reaper exists that
 * periodically evicts expired entries. In addition, if this is configured to be
 * bounded, entries in excess of the max size can be evicted by the reaper based
 * on LRU (Least-Recently-Used) policy.
 *
 * @param <K>
 * @param <V>
 */
public class PersistentCache<K, V> implements ICache<K, V>,
		IEvictionListener<K, PersistentValueWrapper<K, V>>,
		IUpdateListener<K, PersistentValueWrapper<K, V>> {

	private static final Logger logger = Logger
			.getLogger(PersistentCache.class);
	private static final String CLASS_CATALOG_NAME = "saf_java_class_catalog";
	private static int DEFAULT_CACHE_INITIAL_CAPACITY = 1000;
	private static int DEFAULT_CACHE_MAX_CAPACITY = 50000;
	private static float DEFAULT_CACHE_LOAD_FACTOR = 0.75f;

	protected String id = "";

	private Map<K, PersistentValueWrapper<K, V>> cache; // ignore expiration
														// time and modified
														// flag
	private PersistentMap<K, PersistentValueWrapper<K, V>> diskMap;
	private String databaseName;
	private String dbEnvDir;
	private Environment dbEnvironment;
	private DatabaseConfig dbConfig;
	private StoredClassCatalog javaClassCatalog;
	private Database database;

	private IPersistenceDataBinding<K, PersistentValueWrapper<K, V>> dataBinding;
	private boolean isTransactional = true;
	private String jeProperties = "persistence.properties";
	private Durability durability;

	private int diskHighWaterMark = 0;
	private int diskLowWaterMark = 0;
	private int diskReapingIntervalSecs = 60;
	private int diskReapingInitDelaySecs = 10;

	private int defaultExpirySecs = 0;

	private int initialCapacity = DEFAULT_CACHE_INITIAL_CAPACITY;
	private float loadFactor = DEFAULT_CACHE_LOAD_FACTOR;
	private int maxCapacity = DEFAULT_CACHE_MAX_CAPACITY;

	private boolean refreshExpiryOnAccess = false;
	private ScheduledThreadPoolExecutor scheduler;

	public void start() throws Exception {
		_verify();
		_init();
	}

	private void _verify() {
		if (databaseName == null)
			throw new ConfigurationException("database name not specified");

		if (dbEnvDir == null)
			throw new ConfigurationException(
					"database home directory not specified");

		if (dataBinding == null)
			throw new ConfigurationException(
					"cache key/data binding not specified");
	}

	private void _init() throws Exception {
		openDB();

		// disk store
		diskMap = new PersistentMap<K, PersistentValueWrapper<K, V>>(database,
				javaClassCatalog, dataBinding, true, diskHighWaterMark,
				diskLowWaterMark, diskReapingInitDelaySecs,
				diskReapingIntervalSecs);
		diskMap.setId(id);
		diskMap.setRefreshExpiryOnAccess(refreshExpiryOnAccess);
		diskMap.addEvictionListener(this);
		diskMap.addUpdateListener(this);
		if (scheduler != null)
			diskMap.setTimer(scheduler);
		diskMap.init();

		// memory cache
		cache = Collections
				.synchronizedMap(new LinkedHashMap<K, PersistentValueWrapper<K, V>>(
						initialCapacity, loadFactor, true) {

					private static final long serialVersionUID = 827276680854179618L;

					@Override
					protected boolean removeEldestEntry(
							Entry<K, PersistentValueWrapper<K, V>> eldest) {
						return size() > maxCapacity;
					}

				});

	}

	private void openDB() throws Exception {
		logger.info("Opening environment in: " + dbEnvDir);

		try {
			CommonUtils.mkdir(dbEnvDir);
		} catch (IOException e) {
			logger.error("mkdir failed: " + e.getMessage());
			throw new ConfigurationException(e);
		}

		Properties p = CommonUtils.loadProperties(jeProperties);
		EnvironmentConfig env_cfg = new EnvironmentConfig(p);
		env_cfg.setTransactional(isTransactional);
		env_cfg.setAllowCreate(true);
		if (durability != null)
			env_cfg.setDurability(durability);

		this.dbEnvironment = new Environment(new File(dbEnvDir), env_cfg);

		dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(isTransactional);
		dbConfig.setAllowCreate(true);

		// catalog is needed for serial bindings (java serialization)
		Database catalogDb = dbEnvironment.openDatabase(null,
				CLASS_CATALOG_NAME, dbConfig);
		this.javaClassCatalog = new StoredClassCatalog(catalogDb);

		this.database = dbEnvironment
				.openDatabase(null, databaseName, dbConfig);

	}

	/**
	 * Stores the specified key to the specified value in this cache. Neither
	 * the key nor the value can be null.
	 *
	 * @param key
	 * @param val
	 * @param expirySecs
	 *            Number of seconds to keep an entry in the cache and the disk.
	 *            0 means never evict. Note that we can still evict an entry
	 *            with 0 caching time: when we have a bounded cache, we evict in
	 *            order of insertion no matter what the caching time is.
	 * @return the previous value associated with key, or null if there was no
	 *         mapping for key
	 */
	@Override
	public V set(K key, V val, int expirySecs) {
		if (logger.isTraceEnabled())
			logger.trace("set(" + key + ", " + val + ", " + expirySecs + ")");

		PersistentValueWrapper<K, V> wrapper = new PersistentValueWrapper<K, V>(
				key, val, expirySecs);

		PersistentValueWrapper<K, V> retv = diskMap.put(key, wrapper);
		return (retv != null) ? retv.getValue() : null;
	}

	/**
	 * If the specified key is not already associated with a value, associate it
	 * with the given value.
	 *
	 * @param key
	 * @param value
	 * @param expirySecs
	 * @return the previous value associated with the specified key, or null if
	 *         there was no mapping for the key
	 */
	@Override
	public V putIfAbsent(K key, V value, int expirySecs) {

		if (logger.isTraceEnabled())
			logger.trace("putIfAbsent(" + key + ", " + value + ", "
					+ expirySecs + ")");

		PersistentValueWrapper<K, V> wrapper = new PersistentValueWrapper<K, V>(
				key, value, expirySecs);

		PersistentValueWrapper<K, V> retv = diskMap.putIfAbsent(key, wrapper);
		return (retv != null) ? retv.getValue() : null;

	}

	/**
	 * Replaces the entry for a key only if currently mapped to some value.
	 *
	 * @param key
	 * @param value
	 * @param expirationTime
	 * @return the previous value associated with the specified key, or null if
	 *         there was no mapping for the key
	 */
	@Override
	public V replace(K key, V value, int expirySecs) {
		PersistentValueWrapper<K, V> wrapper = new PersistentValueWrapper<K, V>(
				key, value, expirySecs);

		PersistentValueWrapper<K, V> retv = diskMap.replace(key, wrapper);
		return (retv != null) ? retv.getValue() : null;
	}

	@ManagedOperation(description = "Clear local persistent cache")
	@Override
	public void clear() {
		logger.info("clearing persistent cache");
		diskMap.clear();
		cache.clear();
	}

	public void close() {
		diskMap.close();
		database.close();
		javaClassCatalog.close();
		dbEnvironment.close();
		logger.info("closed persistent cache");
	}

	/**
	 * Get the value of the specific key. Optionally to reset the expiry time of
	 * the item stored on disk if refreshExpiry is true.
	 *
	 * @param key
	 * @param refreshExpiry
	 * @return the value to which the specified key is mapped, or null if this
	 *         map contains no mapping for the key
	 */
	public V get(K key, boolean refreshExpiry) {
		PersistentValueWrapper<K, V> retv = cache.get(key);
		if (retv == null) {
			retv = diskMap.get(key, refreshExpiry);
			// cache is updated via listener API
		}

		// Note: we do not refresh expiration time on cache, only for stored
		// item ondisk.

		return (retv != null) ? retv.getValue() : null;
	}

	/**
	 * Get the value of the specific key.
	 */
	@Override
	public V get(K key) {
		PersistentValueWrapper<K, V> retv = cache.get(key);
		if (retv == null) {
			retv = diskMap.get(key);
			// cache is updated via listener API
		}

		return (retv != null) ? retv.getValue() : null;
	}

	public V getAndTouch(K key, int expirationTime) {
		PersistentValueWrapper<K, V> retv = diskMap.getAndTouch(key,
				expirationTime);
		return (retv != null) ? retv.getValue() : null;
	}

	@Override
	public V getAndTouch(K key) {
		PersistentValueWrapper<K, V> retv = diskMap.getAndTouch(key);
		return (retv != null) ? retv.getValue() : null;
	}

	public PersistentValueWrapper<K, V> getEntry(K key) {
		PersistentValueWrapper<K, V> retv = cache.get(key);
		if (retv == null)
			retv = diskMap.getEntry(key);

		return retv;
	}

	public PersistentValueWrapper<K, V> setEntry(K key,
			PersistentValueWrapper<K, V> wrapper) {
		return diskMap.put(key, wrapper);
	}

	@ManagedAttribute
	public int getItemCount() {
		return diskMap.getItemCount();
	}

	@ManagedAttribute
	public String getJePropertiesName() {
		return diskMap.getJePropertiesName();
	}

	@ManagedAttribute
	public long getLastEvictTime() {
		return diskMap.getLastEvictTime();
	}

	/**
	 * Get estimated size.
	 */
	@Override
	public int size() {
		return diskMap.getItemCount();
	}

	/**
	 * Get the number of entries in the persistent store. Note that this call is
	 * expensive. For reporting purpose, it is better to use size().
	 *
	 * @return the number of entries in the persistent store.
	 */
	public int getDiskItemCount() {
		return diskMap.getDiskItemCount();
	}

	public int getInMemoryCount() {
		return cache.size();
	}

	@ManagedAttribute
	public boolean isReapingEnabled() {
		return diskMap.isReapingEnabled();
	}

	/**
	 * Disables the reaper.
	 */
	@ManagedOperation
	public void disableReaping() {
		diskMap.disableReaping();
	}

	@ManagedOperation
	public void enableReaping(int delay, int interval) {
		this.diskReapingInitDelaySecs = delay;
		this.diskReapingIntervalSecs = interval;
		diskMap.enableReaping(delay, interval);
	}

	@ManagedAttribute
	public boolean isRefreshExpiryOnAccess() {
		return diskMap.isRefreshExpiryOnAccess();
	}

	public StoredIterator<Map.Entry<K, PersistentValueWrapper<K, V>>> iterator() {
		return diskMap.iterator();
	}

	public StoredIterator<Map.Entry<Long, PersistentValueWrapper<K, V>>> accessOrderIterator() {
		return diskMap.accessOrderIterator();
	}

	public StoredIterator<Map.Entry<Long, PersistentValueWrapper<K, V>>> expiryOrderIterator() {
		return diskMap.expiryOrderIterator();
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
	public String lookup(String key) {
		PersistentValueWrapper<K, V> retv = cache.get(key);
		if (retv == null)
			return diskMap.lookup(key);
		else
			return retv.value.toString();
	}

	public V set(K key, V value) {
		PersistentValueWrapper<K, V> wrapper = new PersistentValueWrapper<K, V>(
				key, value, this.defaultExpirySecs);

		PersistentValueWrapper<K, V> retv = diskMap.put(key, wrapper);
		return (retv != null) ? retv.getValue() : null;
	}

	public V set(K key, V val, int expirySecs, boolean isModified) {
		if (logger.isTraceEnabled())
			logger.trace("set(" + key + ", " + val + ", " + expirySecs + ")");

		PersistentValueWrapper<K, V> wrapper = new PersistentValueWrapper<K, V>(
				key, val, expirySecs, isModified);

		PersistentValueWrapper<K, V> retv = diskMap.put(key, wrapper);
		return (retv != null) ? retv.getValue() : null;
	}

	public V putIfAbsent(K key, V value) {
		PersistentValueWrapper<K, V> wrapper = new PersistentValueWrapper<K, V>(
				key, value, this.defaultExpirySecs);

		PersistentValueWrapper<K, V> retv = diskMap.putIfAbsent(key, wrapper);
		return (retv != null) ? retv.getValue() : null;
	}

	public V replace(K key, V value) {
		PersistentValueWrapper<K, V> wrapper = new PersistentValueWrapper<K, V>(
				key, value, this.defaultExpirySecs);

		PersistentValueWrapper<K, V> retv = diskMap.replace(key, wrapper);
		return (retv != null) ? retv.getValue() : null;

	}

	@Override
	public void onEviction(K key, PersistentValueWrapper<K, V> value) {
		logger.trace("<onEvict>");
		cache.remove(key);
	}

	public void setDefaultExpirySecs(int defaultExpirySecs) {
		this.defaultExpirySecs = defaultExpirySecs;
	}

	@Override
	public boolean contain(K key) {
		boolean existed = cache.containsKey(key);
		if (existed)
			return true;

		return diskMap.containsKey(key);
	}

	@Override
	public V delete(K key) {
		PersistentValueWrapper<K, V> retv = diskMap.remove(key);
		return (retv != null) ? retv.getValue() : null;
	}

	@Override
	public void onDelete(K key) {
		logger.trace("<onDelete>");
		cache.remove(key);
	}

	@Override
	public void onUpdate(K key, PersistentValueWrapper<K, V> value) {
		logger.trace("<onUpdate>");
		cache.put(key, value);
	}

	@Override
	public void onAdd(K key, PersistentValueWrapper<K, V> value) {
		logger.trace("<onAdd>");
		if (!cache.containsKey(key))
			cache.put(key, value);
	}

	// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

	@ManagedAttribute
	public String getDbEnvDir() {
		return dbEnvDir;
	}

	public void setDbEnvDir(String dbEnvDir) {
		this.dbEnvDir = dbEnvDir;
	}

	@ManagedAttribute
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@ManagedAttribute
	public int getDiskReapingIntervalSecs() {
		return diskReapingIntervalSecs;
	}

	public void setDiskReapingIntervalSecs(int diskReapingIntervalSecs) {
		this.diskReapingIntervalSecs = diskReapingIntervalSecs;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void setRefreshExpiryOnAccess(boolean refreshExpiryOnAccess) {
		this.refreshExpiryOnAccess = refreshExpiryOnAccess;
	}

	public void setDataBinding(
			IPersistenceDataBinding<K, PersistentValueWrapper<K, V>> dataBinding) {
		this.dataBinding = dataBinding;
	}

	@ManagedAttribute
	public int getDiskHighWaterMark() {
		return diskHighWaterMark;
	}

	public void setDiskHighWaterMark(int diskHighWaterMark) {
		this.diskHighWaterMark = diskHighWaterMark;
	}

	@ManagedAttribute
	public int getDiskLowWaterMark() {
		return diskLowWaterMark;
	}

	public void setDiskLowWaterMark(int diskLowWaterMark) {
		this.diskLowWaterMark = diskLowWaterMark;
	}

	@ManagedAttribute
	public int getDefaultExpirySecs() {
		return defaultExpirySecs;
	}

	@ManagedAttribute
	public int getDiskReapingInitDelaySecs() {
		return diskReapingInitDelaySecs;
	}

	@ManagedAttribute
	public void setDiskReapingInitDelaySecs(int diskReapingInitDelaySecs) {
		this.diskReapingInitDelaySecs = diskReapingInitDelaySecs;
	}

	@ManagedAttribute
	public int getCacheSize() {
		return cache.size();
	}

	public boolean isExpired(PersistentValueWrapper<K, V> val) {
		return diskMap.isExpired(val);
	}

	public void setModified(K key, boolean flag) {
		diskMap.setModifiedFlag(key, flag);
	}

	protected ScheduledThreadPoolExecutor getTimer() {
		return diskMap.getTimer();
	}

	public String dump() {
		return diskMap.dump();
	}

	@ManagedAttribute
	public int getInitialCapacity() {
		return initialCapacity;
	}

	public void setInitialCapacity(int initialCapacity) {
		this.initialCapacity = initialCapacity;
	}

	@ManagedAttribute
	public float getLoadFactor() {
		return loadFactor;
	}

	public void setLoadFactor(float loadFactor) {
		this.loadFactor = loadFactor;
	}

	@ManagedAttribute
	public int getMaxCapacity() {
		return maxCapacity;
	}

	public void setMaxCapacity(int maxCapacity) {
		this.maxCapacity = maxCapacity;
	}

	public Transaction beginTransaction() {
		return diskMap.beginTransaction();
	}

	/**
	 * This method is deprecated. Use commitTransaction instead.
	 *
	 * @param transaction
	 */
	public void commit(Transaction transaction) {
		diskMap.commit(transaction);
	}

	public void commitTransaction() {
		diskMap.commitTransaction();
	}

	public void abortTransaction() {
		diskMap.abortTransaction();
	}

	public void setScheduler(ScheduledThreadPoolExecutor scheduler) {
		this.scheduler = scheduler;
	}

	public void setDurability(Durability durability) {
		this.durability = durability;
	}

}
