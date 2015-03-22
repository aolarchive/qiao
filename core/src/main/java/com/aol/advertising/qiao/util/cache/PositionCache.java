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
 * File Name:   PersistentCacheWrapper.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.util.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.management.FileReadingPositionCache;
import com.aol.advertising.qiao.management.FileReadingPositionCache.FileReadState;
import com.aol.advertising.qiao.util.CommonUtils;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredCollection;
import com.sleepycat.collections.StoredIterator;
import com.sleepycat.je.Durability;
import com.sleepycat.je.LockConflictException;

/**
 * PositionCache is a disk-persistent cache that stores a file's read cursor
 * position. Keys of the cache is the checksum value of the file.
 */
public class PositionCache extends
		PersistentCache<Long, FileReadingPositionCache.FileReadState> {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	private String durabilitySettings = "WRITE_NO_SYNC";
	private int maxTries = 3;
	private int lockConflictRetryMsecs = 100;

	public void setDataBinding() {
		super.setDataBinding(_createBinding());
	}

	private IPersistenceDataBinding<Long, PersistentValueWrapper<Long, FileReadState>> _createBinding() {
		return new IPersistenceDataBinding<Long, PersistentValueWrapper<Long, FileReadState>>() {

			@Override
			public EntryBinding<Long> getKeyBinding(
					StoredClassCatalog clzCatalog) {
				return TupleBinding.getPrimitiveBinding(Long.class);
			}

			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			public EntryBinding<PersistentValueWrapper<Long, FileReadState>> getValueBinding(
					StoredClassCatalog clzCatalog) {
				return new SerialBinding(clzCatalog,
						PersistentValueWrapper.class);
			}

		};
	}

	@Override
	public void start() throws Exception {
		if (isStarted.compareAndSet(false, true)) {
			if (durabilitySettings != null) {
				try {
					Durability durability = Durability
							.parse(durabilitySettings);
					if (durability != null)
						super.setDurability(durability);
				} catch (IllegalArgumentException e) {
					logger.warn("invalid durability string "
							+ durabilitySettings + " => " + e.getMessage()
							+ ", ignored the settings.");
				}
			}
			super.start();
		}
	}

	public int copyNewRecordsFrom(PositionCache other) {

		int count = 0;
		for (int i = 0; i < maxTries;) {
			try {
				count = copyNewRecordsFromSingleTry(other);
				break;
			} catch (LockConflictException e) {
				logger.info("<LockConflictException> #" + i);
				if (i++ < maxTries) {
					if (logger.isDebugEnabled())
						logger.debug(e.getClass().getSimpleName() + ": "
								+ e.getMessage());

					CommonUtils.sleepQuietly(lockConflictRetryMsecs);
					continue;
				} else
					throw e;
			}
		}
		return count;
	}

	private int copyNewRecordsFromSingleTry(PositionCache other) {

		int count = 0;
		StoredIterator<Map.Entry<Long, PersistentValueWrapper<Long, FileReadingPositionCache.FileReadState>>> iter = other
				.iterator();
		try {
			StoredCollection<Entry<Long, PersistentValueWrapper<Long, FileReadState>>> collection = iter
					.getCollection();
			iter.close();
			iter = null;

			Iterator<Entry<Long, PersistentValueWrapper<Long, FileReadState>>> coll_iter = collection
					.iterator();

			while (coll_iter.hasNext()) {
				Entry<Long, PersistentValueWrapper<Long, FileReadingPositionCache.FileReadState>> entry = coll_iter
						.next();
				long key = entry.getKey();
				FileReadState my_state = this.get(key);
				FileReadState other_state = entry.getValue().getValue();
				if (my_state == null
						|| my_state.timestamp < other_state.timestamp) {
					FileReadState rc = this.set(key, other_state);
					if (rc == null) {
						count++;
						logger.info("position copied: "
								+ other_state.toString());
					}
				}
			}
		} finally {
			// iterator must be explicitly closed
			if (iter != null)
				iter.close();
		}

		return count;
	}

	public void setDurabilitySettings(String durabilitySettings) {
		this.durabilitySettings = durabilitySettings;
	}

	public String getDurabilitySettings() {
		return durabilitySettings;
	}

	public void setMaxTries(int maxTries) {
		this.maxTries = maxTries;
	}

	public void setLockConflictRetryMsecs(int lockConflictRetryMsecs) {
		this.lockConflictRetryMsecs = lockConflictRetryMsecs;
	}
}
