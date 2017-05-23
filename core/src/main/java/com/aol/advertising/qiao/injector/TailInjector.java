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

package com.aol.advertising.qiao.injector;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.agent.IDataPipe;
import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.injector.file.AbstractFileTailer;
import com.aol.advertising.qiao.injector.file.AvroFileTailer;
import com.aol.advertising.qiao.injector.file.BinaryFileTailer;
import com.aol.advertising.qiao.injector.file.ICallback;
import com.aol.advertising.qiao.injector.file.IFileOperationListener;
import com.aol.advertising.qiao.injector.file.ITailer.TAIL_MODE;
import com.aol.advertising.qiao.injector.file.ITailerDataHandler;
import com.aol.advertising.qiao.injector.file.TextBlockFileTailer;
import com.aol.advertising.qiao.injector.file.TextFileTailer;
import com.aol.advertising.qiao.management.FileLockManager;
import com.aol.advertising.qiao.management.FileReadingPositionCache;
import com.aol.advertising.qiao.management.IStatsCalculatorAware;
import com.aol.advertising.qiao.management.QiaoFileBookKeeper;
import com.aol.advertising.qiao.management.metrics.IStatisticsStore;
import com.aol.advertising.qiao.management.metrics.PubStats;
import com.aol.advertising.qiao.management.metrics.PubStats.StatType;
import com.aol.advertising.qiao.management.metrics.StatsCalculator;
import com.aol.advertising.qiao.management.metrics.StatsCollector;
import com.aol.advertising.qiao.management.metrics.StatsEnum;
import com.aol.advertising.qiao.management.metrics.StatsEvent;
import com.aol.advertising.qiao.management.metrics.StatsEvent.StatsOp;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.IntervalMetric;
import com.aol.advertising.qiao.util.StatsUtils;
import com.aol.advertising.qiao.util.cache.PositionCache;

/**
 * TailInjector reads and follows a file in a way similar to the Unix 'tail -F'
 * command. It supports the following features:
 * <p>
 *
 * <pre>
 * A) follows a log when the log rotated, similar to 'tail -F'.
 * B) resumes at where it previously left off after QIAO is restarted if the file has not been rotated off.
 * C) starts from the beginning of the file if the previous file has been rotated before QIAO restarts.
 * D) accepts listeners which will be notified whenever a file is open for processing or after a file
 *    has been complete.
 * </pre>
 *
 * </p>
 * <p>
 * Note that this class requires a directory in the file system to store the
 * read cursor position on disk for every read operation via PositionCache.
 * </p>
 * <p>
 * tailerMode should be set to either TEXTBLOCK and BINARY. Either mode, file
 * content is read one block at a time up to predefined buffer size (bufSize),
 * default to 4096 (bytes). It is up to a user-supplied data handler to decipher
 * application-specific data in BINARY mode. In TEXTBLOCK, data handler is
 * optional. By default data is broken up line by line (a single ‘line’ of text
 * followed by line feed (‘\n’)).
 * </p>
 *
 * @param <T>
 *            data buffer format: String or ByteBuffer
 */
@ManagedResource(description = "Tail Injector")
public class TailInjector<T> implements IDataInjector, IInjectBookKeeper,
        IInjectPositionCache, IStatsCalculatorAware
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    // input properties
    private String filename;
    private long delayMillis = 10; // default
    private int bufSize = 4096;
    private long fileCheckDelayMillis = 1000;
    private ITailerDataHandler<T> dataHandler; // optional data handler
    private boolean autoCommitFilePosition = false; // note: je auto-commit is unstable
    private String durabilitySettings;
    private int cacheDefaultExpirySecs = 86400;
    private int cacheDiskReapingIntervalSecs = 60;
    private int cacheDiskReapingInitDelaySecs = 60;

    private AbstractFileTailer< ? > tailer;
    private Thread tailerThread;
    private AtomicBoolean running = new AtomicBoolean(false);
    private AtomicBoolean isSuspended = new AtomicBoolean(false);

    private PositionCache positionCache;
    private FileReadingPositionCache fileReadPosition;

    private ApplicationEventPublisher applicationEventPublisher;
    private AtomicLong numInputs = new AtomicLong(0);
    private AtomicLong numFiles = new AtomicLong(0);
    protected InjectorStatus status = InjectorStatus.INACTIVE;
    private boolean startReadingFromFileEnd = false; // only apply to TEXT mode
    private TAIL_MODE tailMode = TAIL_MODE.TEXT;

    private IDataPipe dataPipe;
    private ICallback callback; // internal use for adding data to pipe
    private String id;
    private String funnelId;

    private String statKeyInputs = StatsEnum.PROCESSED_BLOCKS.value();
    private String statKeyFiles = StatsEnum.PROCESSED_FILES.value();
    private String statKeyFileTime = StatsEnum.FILE_PROCESSING_TIME.value();
    private IntervalMetric fpStats;

    private StatsCollector statsCollector;
    private StatsCalculator statsCalculator;
    private Map<String, PubStats> counterKeys = new LinkedHashMap<String, PubStats>();

    private QiaoFileBookKeeper bookKeeper; // notified when complete processing the current file after rotated off
    private boolean enableFileWatcher = true;
    private int checksumByteLength = 2048;

    private FileLockManager fileLockManager;
    private List<IFileOperationListener> listeners = new ArrayList<IFileOperationListener>();


    @Override
    public void init() throws Exception
    {
        _validate();

        File file_to_tail = new File(filename);

        _setupPositionCache();

        _setupCallback();

        fpStats = new IntervalMetric(statKeyFileTime);

        _setupTailerDelegate(file_to_tail);

        _registerStatsCollector();

        _registerStatsCalculator();

        logger.info(this.getClass().getName() + " initialized");

    }


    private void _validate()
    {

        if (filename == null)
            throw new ConfigurationException("filename not defined");

        //if (filePattern == null)
        //    throw new ConfigurationException("filePattern not defined");

        if (dataPipe == null)
            throw new ConfigurationException("dataPipe not set");

        if (bookKeeper == null)
            throw new ConfigurationException("bookKeeper not set");

        if (fileLockManager == null)
            throw new ConfigurationException("fileLockManager not set");

        if (positionCache == null)
            throw new ConfigurationException("positionCache not set");
    }


    private void _setupPositionCache() throws Exception
    {
        if (durabilitySettings != null)
            positionCache.setDurabilitySettings(durabilitySettings);
        positionCache.setDefaultExpirySecs(this.cacheDefaultExpirySecs);
        positionCache
                .setDiskReapingInitDelaySecs(cacheDiskReapingInitDelaySecs);
        positionCache.setDiskReapingIntervalSecs(cacheDiskReapingIntervalSecs);
        fileReadPosition = new FileReadingPositionCache(positionCache);
        positionCache.start();

    }


    private void _setupCallback()
    {
        if (callback == null)
        {
            callback = new ICallback()
            {
                private IDataPipe pipe;


                @Override
                public void receive(Object data)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("recv> " + data.toString());

                    pipe.write(data);
                }


                @Override
                public void setDataPipe(IDataPipe pipe)
                {
                    this.pipe = pipe;
                }

            };

            callback.setDataPipe(dataPipe);
        }
    }


    @SuppressWarnings("unchecked")
    private void _setupTailerDelegate(File fileToTail) throws Exception
    {
        logger.info("tailer mode: " + tailMode);
        switch (tailMode)
        {
            case TEXTBLOCK:
                tailer = TextBlockFileTailer.create(fileToTail, delayMillis,
                        bufSize, fileReadPosition,
                        (ITailerDataHandler<String>) dataHandler,
                        startReadingFromFileEnd);
                break;
            case TEXT:
                tailer = TextFileTailer.create(fileToTail, delayMillis,
                        bufSize, fileReadPosition,
                        (ITailerDataHandler<String>) dataHandler,
                        startReadingFromFileEnd);
                break;
            case BINARY:
                tailer = BinaryFileTailer.create(fileToTail, delayMillis,
                        bufSize, fileReadPosition,
                        (ITailerDataHandler<ByteBuffer>) dataHandler);
                break;
            case AVRO:
                tailer = AvroFileTailer.create(fileToTail, delayMillis,
                        bufSize, fileReadPosition,
                        (ITailerDataHandler<ByteBuffer>) dataHandler);
                break;
            default:
                throw new ConfigurationException("invalid tailer mode: "
                        + tailMode);
        }

        tailer.setNumInputs(numInputs);
        tailer.setNumFiles(numFiles);
        tailer.setCallback(callback);
        tailer.setChecksumByteLength(checksumByteLength);
        tailer.setFileCheckDelayMillis(fileCheckDelayMillis);
        tailer.setFileLockManager(fileLockManager);
        tailer.setFpStats(fpStats);
        tailer.setAutoCommitFilePosition(autoCommitFilePosition);
        tailer.registerListener(bookKeeper);
        for (IFileOperationListener lis : listeners)
            tailer.registerListener(lis);

        tailer.init();

    }


    private void _registerStatsCollector()
    {
        if (statsCollector != null)
        {
            final String src = this.getClass().getSimpleName();
            statsCollector.register(new Callable<Void>()
            {

                @Override
                public Void call()
                {
                    if (numInputs.get() > 0)
                    {
                        applicationEventPublisher.publishEvent(new StatsEvent(
                                this, src, funnelId, StatsOp.INCRBY,
                                statKeyInputs, numInputs.getAndSet(0)));
                    }

                    if (numFiles.get() > 0)
                    {
                        applicationEventPublisher.publishEvent(new StatsEvent(
                                this, src, funnelId, StatsOp.INCRBY,
                                statKeyFiles, numFiles.getAndSet(0)));
                    }

                    applicationEventPublisher.publishEvent(new StatsEvent(this,
                            src, funnelId, StatsOp.INCRBY_INTVL_STAT,
                            statKeyFileTime, fpStats.getAndReset()));

                    return null;
                }

            });
        }

    }


    private void _registerStatsCalculator()
    {
        if (statsCalculator != null)
        {
            IStatisticsStore statsStore = StatsUtils.getStatsStore(funnelId);
            if (statsStore == null)
                throw new ConfigurationException(funnelId
                        + " statistics store does not exist");

            if (counterKeys.size() == 0)
            {
                PubStats pstats = new PubStats(statKeyFiles, true, false,
                        false, false); // raw
                counterKeys.put(pstats.getMetric(), pstats);

                pstats = new PubStats(statKeyInputs, false, false, true, false); // diff
                counterKeys.put(pstats.getMetric(), pstats);

                pstats = new PubStats(StatType.INTERVAL_METRIC,
                        statKeyFileTime, false, true, false, false); // avg
                counterKeys.put(pstats.getMetric(), pstats);

            }

            statsCalculator.register(statsCalculator.new CalcCallable(
                    statsStore, counterKeys));
        }

    }


    @Override
    public void start() throws Exception
    {
        if (running.compareAndSet(false, true))
        {

            tailer.start();

            tailerThread = new Thread(tailer);
            tailerThread.setDaemon(true);
            tailerThread.setName(CommonUtils.resolveThreadName("TailInjector"));
            tailerThread.start();

            status = InjectorStatus.ACTIVE;
            logger.info(this.getClass().getSimpleName() + " started");
        }
    }


    @Override
    public void shutdown()
    {
        if (running.compareAndSet(true, false))
        {
            logger.info("shutting down...");
            tailer.stop();
            try
            {
                tailerThread.join(5000); // wait at most 5s for thread to die
            }
            catch (InterruptedException e)
            {
            }

            logger.info(">last read file position: " + fileReadPosition.get());

            fileReadPosition.close();

            status = InjectorStatus.INACTIVE;

            logger.info(this.getClass().getName() + " terminated");

        }
    }


    @ManagedAttribute
    @Override
    public boolean isRunning()
    {
        return running.get() && tailerThread.isAlive()
                && (status == InjectorStatus.ACTIVE);
    }


    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher applicationEventPublisher)
    {
        this.applicationEventPublisher = applicationEventPublisher;
    }


    @ManagedOperation
    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            tailer.stop();
            try
            {
                tailerThread.join(5000); // wait at most 5s for thread to die
            }
            catch (InterruptedException e)
            {
            }

            logger.info(">last read file position: " + fileReadPosition.get());

            status = InjectorStatus.SUSPENDED;

            logger.info(this.getClass().getSimpleName() + " suspended");

        }
    }


    @ManagedOperation
    @Override
    public void resume()
    {
        if (isSuspended.compareAndSet(true, false))
        {
            tailer.start();
            try
            {
                tailerThread = new Thread(tailer);
                tailerThread.setDaemon(true);
                tailerThread.setName(CommonUtils
                        .resolveThreadName("TailInjector"));
                tailerThread.start();

                status = InjectorStatus.ACTIVE;
                logger.info(this.getClass().getSimpleName() + " resumed");
            }
            catch (Exception e)
            {
                logger.error(
                        "failed to resume the opration => " + e.getMessage(), e);
            }

        }

    }


    public void setFilename(String filename)
    {
        this.filename = filename;
    }


    public void setDelayMillis(long delayMillis)
    {
        this.delayMillis = delayMillis;
    }


    public void setBufSize(int bufSize)
    {
        this.bufSize = bufSize;
    }


    public void setStatsCollector(StatsCollector statsCollector)
    {
        this.statsCollector = statsCollector;
    }


    @ManagedAttribute
    public String getFilename()
    {
        return filename;
    }


    @ManagedAttribute
    public long getDelayMillis()
    {
        return delayMillis;
    }


    @ManagedAttribute
    public int getBufSize()
    {
        return bufSize;
    }


    @ManagedAttribute
    public long getFileReadOffset()
    {
        return fileReadPosition.get();
    }


    public AtomicLong getNumInput()
    {
        return numInputs;
    }


    public boolean isStartReadingFromFileEnd()
    {
        return startReadingFromFileEnd;
    }


    public void setStartReadingFromFileEnd(boolean startReadingFromFileEnd)
    {
        this.startReadingFromFileEnd = startReadingFromFileEnd;
    }


    /**
     * @param tailerMode
     *            TEXTBLOCK, BINARY, or TEXT
     */
    public void setTailMode(String tailerMode)
    {
        this.tailMode = TAIL_MODE.valueOf(tailerMode);
    }


    public ITailerDataHandler<T> getDataHandler()
    {
        return dataHandler;
    }


    /**
     * Set user data handler (optional). For TailerMode.BINARY, entire buffer is
     * passed to the data handler. For TailerMode.TEXTBLOCK, a data block is
     * split by line-end(s) before passing them to data handler.
     *
     * @param dataHandler
     */
    public void setDataHandler(ITailerDataHandler<T> dataHandler)
    {
        this.dataHandler = dataHandler;
    }


    /**
     * For internal use.
     */
    @Override
    public void setDataPipe(IDataPipe dataPipe)
    {
        this.dataPipe = dataPipe;
    }


    @Override
    public void run()
    {
        // N/A
    }


    /**
     * For internal use.
     *
     * @param callback
     */
    public void setCallback(ICallback callback)
    {
        this.callback = callback;
    }


    @Override
    public void setId(String id)
    {
        this.id = id;
    }


    @Override
    public String getId()
    {
        return id;
    }


    @Override
    public void setFunnelId(String funnelId)
    {
        this.funnelId = funnelId;
    }


    public void setStatKeyInputs(String statKeyInputs)
    {
        this.statKeyInputs = statKeyInputs;
    }


    public void setStatKeyFiles(String statKeyFiles)
    {
        this.statKeyFiles = statKeyFiles;
    }


    @Override
    public void setStatsCalculator(StatsCalculator statsCalculator)
    {
        this.statsCalculator = statsCalculator;
    }


    @ManagedOperation
    public String getCurrentFileModTimeAndReadTime()
    {
        return tailer.getCurrentFileModTimeAndReadTime();
    }


    @ManagedOperation
    public long computeCurrentFileChecksum()
    {
        return tailer.getCurrentFileChecksum();
    }


    @ManagedAttribute
    public long getCurrentProcessingFileChecksum()
    {
        return tailer.getCurrentReadingFileChecksum();
    }


    public boolean isEnableFileWatcher()
    {
        return enableFileWatcher;
    }


    public void setEnableFileWatcher(boolean enableFileWatcher)
    {
        this.enableFileWatcher = enableFileWatcher;
    }


    @ManagedAttribute
    public int getChecksumByteLength()
    {
        return checksumByteLength;
    }


    public void setChecksumByteLength(int checksumByteLength)
    {
        this.checksumByteLength = checksumByteLength;
    }


    @ManagedAttribute
    public long getFileCheckDelayMillis()
    {
        return fileCheckDelayMillis;
    }


    public void setFileCheckDelayMillis(long fileCheckDelayMillis)
    {
        this.fileCheckDelayMillis = fileCheckDelayMillis;
    }


    @Override
    public void setFileBookKeeper(QiaoFileBookKeeper bookKeeper)
    {
        this.bookKeeper = bookKeeper;
    }


    @ManagedAttribute
    public String getFunnelId()
    {
        return funnelId;
    }


    public void setFileLockManager(FileLockManager lockManager)
    {
        this.fileLockManager = lockManager;
    }


    @ManagedOperation
    public String dumpBookKeeperCache()
    {
        return bookKeeper.dumpHistoryCache();
    }


    @ManagedOperation
    public String dumpPositionCache()
    {
        return fileReadPosition.dumpCache();
    }


    @Override
    public boolean isSuspended()
    {
        return isSuspended.get();
    }


    public void setPositionCache(PositionCache positionCache)
    {
        this.positionCache = positionCache;
    }


    @Override
    public PositionCache getPositionCache()
    {
        return this.positionCache;
    }


    public void setStatKeyFileTime(String statKeyFileTime)
    {
        this.statKeyFileTime = statKeyFileTime;
    }


    @ManagedAttribute
    public boolean isAutoCommitFilePosition()
    {
        return autoCommitFilePosition;
    }


    public void setAutoCommitFilePosition(boolean autoCommitFilePosition)
    {
        this.autoCommitFilePosition = autoCommitFilePosition;
    }


    public void setDurabilitySettings(String durabilitySettings)
    {
        this.durabilitySettings = durabilitySettings;
    }


    public String getDurabilitySettings()
    {
        return durabilitySettings;
    }


    @ManagedAttribute
    public String getPositionCacheDurability()
    {
        return positionCache.getDurabilitySettings();
    }


    /**
     * Register listener to be notified if a file is open or finished. Note that
     * only those listeners registered before init() will be notified.
     *
     * @param listener
     */
    public void registerListener(IFileOperationListener listener)
    {
        this.listeners.add(listener);
    }


    @ManagedAttribute
    public int getCacheDefaultExpirySecs()
    {
        return cacheDefaultExpirySecs;
    }


    public void setCacheDefaultExpirySecs(int cacheDefaultExpirySecs)
    {
        this.cacheDefaultExpirySecs = cacheDefaultExpirySecs;
    }


    @ManagedAttribute
    public int getCacheDiskReapingIntervalSecs()
    {
        return cacheDiskReapingIntervalSecs;
    }


    public void setCacheDiskReapingIntervalSecs(int cacheDiskReapingIntervalSecs)
    {
        this.cacheDiskReapingIntervalSecs = cacheDiskReapingIntervalSecs;
    }


    public int getCacheDiskReapingInitDelaySecs()
    {
        return cacheDiskReapingInitDelaySecs;
    }


    public void setCacheDiskReapingInitDelaySecs(
            int cacheDiskReapingInitDelaySecs)
    {
        this.cacheDiskReapingInitDelaySecs = cacheDiskReapingInitDelaySecs;
    }
}
