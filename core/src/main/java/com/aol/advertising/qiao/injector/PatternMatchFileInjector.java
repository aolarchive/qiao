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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.agent.IDataPipe;
import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.exception.QuarantineException;
import com.aol.advertising.qiao.injector.file.AbstractFileReader;
import com.aol.advertising.qiao.injector.file.AvroFileReader;
import com.aol.advertising.qiao.injector.file.BinaryFileReader;
import com.aol.advertising.qiao.injector.file.DoneFileHandler;
import com.aol.advertising.qiao.injector.file.ICallback;
import com.aol.advertising.qiao.injector.file.IFileOperationListener;
import com.aol.advertising.qiao.injector.file.IFileReader.READ_MODE;
import com.aol.advertising.qiao.injector.file.ITailerDataHandler;
import com.aol.advertising.qiao.injector.file.TextBlockFileReader;
import com.aol.advertising.qiao.injector.file.watcher.QiaoFileManager;
import com.aol.advertising.qiao.management.FileLockManager;
import com.aol.advertising.qiao.management.FileReadingPositionCache;
import com.aol.advertising.qiao.management.IStatsCalculatorAware;
import com.aol.advertising.qiao.management.ISuspendable;
import com.aol.advertising.qiao.management.QiaoFileBookKeeper;
import com.aol.advertising.qiao.management.QiaoFileEntry;
import com.aol.advertising.qiao.management.QuarantineFileHandler;
import com.aol.advertising.qiao.management.ToolsStore;
import com.aol.advertising.qiao.management.metrics.IStatisticsStore;
import com.aol.advertising.qiao.management.metrics.PubStats;
import com.aol.advertising.qiao.management.metrics.PubStats.StatType;
import com.aol.advertising.qiao.management.metrics.StatsCalculator;
import com.aol.advertising.qiao.management.metrics.StatsCollector;
import com.aol.advertising.qiao.management.metrics.StatsEnum;
import com.aol.advertising.qiao.management.metrics.StatsEvent;
import com.aol.advertising.qiao.management.metrics.StatsEvent.StatsOp;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.FileFinder;
import com.aol.advertising.qiao.util.IntervalMetric;
import com.aol.advertising.qiao.util.StatsUtils;
import com.aol.advertising.qiao.util.cache.PositionCache;

/**
 * PatternMatchFileInjector reads data from a collection of files whose name
 * matches a predefined pattern within a directory. When a file is finished
 * processing, it is moved to another so called "done" directory.
 * <p>
 * The readMode property needs to be set to either TEXTBLOCK and BINARY. Either
 * mode, file data is read one block at a time up to predefined buffer size
 * (bufSize), default to 4096 (bytes). It is up to a user-supplied data handler
 * to decipher application-specific data in BINARY mode. In TEXTBLOCK, data
 * handler is optional. By default data is broken up line by line (a single
 * ‘line’ of text followed by line feed (‘\n’)). *
 * </p>
 * <p>
 * PatternMatchFileInjector records read cursor position as a file is being
 * processed. As such if QIAO is restarted, the unfinished file can be processed
 * from the position it was previously read. As such, it requires a directory in
 * the file system to store the information.
 * </p>
 * <p>
 * Note that PatternMatchFileInjector is not intended to process those files
 * that are still actively being written by the log producer. You need to make
 * sure the defined file pattern follows this rule and does not pick up the
 * active files.
 * </p>
 * <p>
 * PatternMatchFileInjector accepts listeners which will be notified whenever a
 * file is open for processing or after a file has been complete.
 * </p>
 *
 * @param <T>
 *            data buffer format: String or ByteBuffer
 */
@ManagedResource
public class PatternMatchFileInjector<T> implements IDataInjector, //IInjectBookKeeper,
        IInjectPositionCacheDependency, ISuspendable, IStatsCalculatorAware
{
    public enum FileReadStatus
    {
        COMPLETE, NOT_STARTED, INCOMPLETE
    }

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // input properties
    private String srcDir; // source file directory
    private String filePattern; // file name matching pattern
    private long fileCheckDelayMillis = 1000; // wait time for files to be
                                              // available
    private READ_MODE readMode = READ_MODE.TEXTBLOCK; // read processing mode
    private int bufSize = 4096; // read buffer size
    private int maxFilesToFind = -1;
    private boolean autoCommitFilePosition = false; // note: je auto-commit is
                                                    // unstable
    private String durabilitySettings;
    private int cacheDefaultExpirySecs = 86400;
    private int cacheDiskReapingIntervalSecs = 60;
    private int cacheDiskReapingInitDelaySecs = 60;

    private FileFinder finder;
    private ExecutorService executor;
    private Path srcDirPath;

    private AbstractFileReader< ? > fileReader;
    private ITailerDataHandler<T> dataHandler;

    private AtomicBoolean running = new AtomicBoolean(false);
    private AtomicBoolean isSuspended = new AtomicBoolean(false);

    private PositionCache positionCache;
    private FileReadingPositionCache fileReadPosition;

    private ApplicationEventPublisher applicationEventPublisher;
    private AtomicLong numInputs = new AtomicLong(0);
    private AtomicLong numFiles = new AtomicLong(0);
    protected InjectorStatus status = InjectorStatus.INACTIVE;

    private IDataPipe dataPipe;
    private ICallback callback; // internal use for adding data to pipe
    private String id;
    private String funnelId;
    private String agentId;

    private String statKeyInputs = StatsEnum.PROCESSED_BLOCKS.value();
    private String statKeyFiles = StatsEnum.PROCESSED_FILES.value();
    private StatsCollector statsCollector;
    private StatsCalculator statsCalculator;
    private Map<String, PubStats> counterKeys = new LinkedHashMap<String, PubStats>();

    private QiaoFileBookKeeper bookKeeper;
    private DoneFileHandler doneFileHandler;
    private int checksumByteLength = 2048;
    private String currentFile;
    private long currentReadFileChecksum;

    private Set<Path> matchedFileSet = new HashSet<Path>();

    private FileLockManager fileLockManager;
    private PositionCache initPositionCacheFrom;

    private String statKeyFileTime = StatsEnum.FILE_PROCESSING_TIME.value();
    private IntervalMetric fpStats;
    private List<IFileOperationListener> listeners = new ArrayList<IFileOperationListener>();
    private QuarantineFileHandler quarantineFileHandler;


    @Override
    public void init() throws Exception
    {
        _validate();

        srcDirPath = Paths.get(srcDir);

        _setupFileFinder();

        _setupPositionCache();

        _setupCallback();

        _registerStatsCollector();

        _registerStatsCalculator();

        fileReader = _createReader();

        logger.info(this.getClass().getName() + " initialized");

    }


    private void _validate()
    {

        if (srcDir == null)
            throw new ConfigurationException("srcDir not defined");

        if (filePattern == null)
            throw new ConfigurationException("filePattern not defined");

        if (dataPipe == null)
            throw new ConfigurationException("dataPipe not set");

        if (positionCache == null)
            throw new ConfigurationException("positionCache not set");

        QiaoFileManager fm = ToolsStore.getFileManager(agentId);
        if (null == fm)
            throw new ConfigurationException(
                    "Missing file manager for agent " + agentId);

        doneFileHandler = fm.getDoneFileHandler();
        if (null == doneFileHandler)
            throw new ConfigurationException(
                    "Missing doneFileHandler for agent " + agentId);

        quarantineFileHandler = fm.getQuarantineFileHandler();
        if (null == quarantineFileHandler)
            throw new ConfigurationException(
                    "Missing quarantineFileHandler for agent " + agentId);

        bookKeeper = fm.getBookKeeper();
        if (bookKeeper == null)
            throw new ConfigurationException(
                    "Missing bookKeeper for agent " + agentId);

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


    private void _setupFileFinder()
    {
        finder = new FileFinder(filePattern);
        finder.setMaxFiles(maxFilesToFind);
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


    private FileReadingPositionCache getPoistionFromCache(long key,
            String filename) throws Exception
    {
        fileReadPosition.setKey(key);

        FileReadingPositionCache.FileReadState fstate = fileReadPosition
                .getReadState();
        String s = String.format(
                "LAST PROCESSING STATUS: file=%s, timestamp=%s, read_position=%d, checksum=%d",
                filename, CommonUtils.getFriendlyTimeString(fstate.timestamp),
                fstate.position, fstate.checksum);
        logger.info(s);

        return fileReadPosition;
    }


    @SuppressWarnings("unchecked")
    private AbstractFileReader< ? > _createReader() throws Exception
    {

        logger.info("read mode: " + readMode);

        AbstractFileReader< ? > file_reader;
        switch (readMode)
        {
            case TEXTBLOCK:
                file_reader = TextBlockFileReader.create(bufSize,
                        (ITailerDataHandler<String>) dataHandler);
                break;
            case BINARY:
                file_reader = BinaryFileReader.create(bufSize,

                        (ITailerDataHandler<ByteBuffer>) dataHandler);
                break;
            case AVRO:
                file_reader = AvroFileReader.create(bufSize,
                        (ITailerDataHandler<ByteBuffer>) dataHandler);
                break;
            default:
                throw new ConfigurationException(
                        "invalid tailer mode: " + readMode);
        }

        file_reader.setNumInputs(numInputs);
        file_reader.setNumFiles(numFiles);
        file_reader.setCallback(callback);
        file_reader.setChecksumByteLength(checksumByteLength);
        file_reader.setAutoCommitFilePosition(autoCommitFilePosition);
        file_reader.registerListener(bookKeeper); // bookKeeper must be
                                                  // registered first
        for (IFileOperationListener lis : listeners)
            file_reader.registerListener(lis);

        file_reader.init();

        return file_reader;

    }


    private void _registerStatsCollector()
    {
        fpStats = new IntervalMetric(statKeyFileTime);
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
                throw new ConfigurationException(
                        funnelId + " statistics store does not exist");

            if (counterKeys.size() == 0)
            {
                PubStats pstats = new PubStats(statKeyFiles, true, false, false,
                        false); // raw
                counterKeys.put(pstats.getMetric(), pstats);

                pstats = new PubStats(statKeyInputs, false, false, true, false); // diff
                counterKeys.put(pstats.getMetric(), pstats);

                pstats = new PubStats(StatType.INTERVAL_METRIC, statKeyFileTime,
                        false, true, false, false); // avg
                counterKeys.put(pstats.getMetric(), pstats);

            }

            statsCalculator.register(
                    statsCalculator.new CalcCallable(statsStore, counterKeys));
        }

    }


    @Override
    public void start() throws Exception
    {
        if (running.compareAndSet(false, true))
        {

            fileReader.start();

            executor = CommonUtils.createSingleThreadExecutor("FileInjector");
            executor.execute(this);

            status = InjectorStatus.ACTIVE;
            logger.info(this.getClass().getSimpleName() + " started");
        }
    }


    /**
     * <pre>
     * Logic flow:
     * - Get next file that matches the pattern
     * - Compute file's checksum
     * - With the checksum, wait until the lock is acquired
     * - Check if the file processing is done from BookKeeper
     * - If done, return
     * - Otherwise rename the file to include checksum
     * - Prepare file reader
     * - File reader processing the file until it is done
     * </pre>
     */
    @Override
    public void run()
    {

        while (running.get())
        {
            Path file = null;
            try
            {
                file = getNextFile();
                if (file == null)
                {
                    CommonUtils.sleepQuietly(fileCheckDelayMillis);
                    continue;
                }

                if (Files.notExists(file))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("file " + file + " does not exist");
                    continue;
                }

                processFileAndTimeIt(file);

            }
            catch (InterruptedException e)
            {
                logger.info("interrupted");
            }
            catch (FileNotFoundException e)
            {
                // already moved by file manager
            }
            catch (NoSuchFileException e)
            {
                // already moved by file manager
            }
            catch (QuarantineException e)
            {
                logger.error(e.getMessage(), e);

                if (file != null)
                    quarantine(file, currentReadFileChecksum);
            }
            catch (Exception e)
            {
                logger.error(e.getMessage(), e);
            }

            currentFile = null;
            currentReadFileChecksum = 0;

            CommonUtils.sleepQuietly(fileCheckDelayMillis);
        }

        logger.info(this.getClass().getSimpleName() + " terminated");
    }


    private void processFileAndTimeIt(Path file) throws Exception
    {

        logger.info("processing " + file.toString());
        File fileToTail = file.toFile();

        long checksum = CommonUtils.checksumOptionalylUseFileLength(fileToTail,
                checksumByteLength);

        tryAcquireLock(checksum); // ------------

        // check history and see if it has been processed
        QiaoFileEntry history = bookKeeper.getHistory(checksum);
        if (history != null && history.isCompleted())
        {
            // already finished -> move the file and return
            logger.info("file " + file.toString() + " was done");
            doneFileHandler.moveFileToDoneDirIfExists(file, checksum);
            return;
        }

        // -----------------------------------

        // rename the file to contain checksum value if not there already
        if (!doneFileHandler.nameContainsChecksum(file, checksum))
        {
            Path new_path = doneFileHandler.renameFileToIncludeChecksum(file,
                    checksum);
            file = new_path;
        }

        currentFile = file.toString(); // for jmx
        currentReadFileChecksum = checksum;

        long ts_start = System.currentTimeMillis();
        boolean is_done = processFile(file, checksum, history);
        if (is_done)
        {
            long dur = System.currentTimeMillis() - ts_start;
            fpStats.update(dur);
            logger.info(String.format("file processing time: %.3f secs",
                    dur / 1000.0));

            fileReadPosition.remove(currentReadFileChecksum);
            if (logger.isDebugEnabled())
                logger.debug("removed " + currentReadFileChecksum
                        + " from positionCache");
        }
    }


    /**
     * Start reading the content of the file. Resume from last read offset if it
     * was processed previously.
     *
     * @param filePath
     * @return true if file processing is complete, false otherwise.
     * @throws Exception
     */
    private boolean processFile(Path filePath, long checksum,
            QiaoFileEntry historyEntry) throws Exception
    {

        File file_to_tail = filePath.toFile();

        // make sure we have most up-to-date record
        copyNewOrUpdatedRecordsFromDependentCache();

        FileReadStatus read_status = setupFileReader(file_to_tail, checksum,
                historyEntry);
        if (read_status == FileReadStatus.COMPLETE)
            return true;

        boolean is_done = fileReader.execute();
        if (is_done)
            doneFileHandler.moveFileToDoneDir(filePath, checksum);

        return is_done;
    }


    /**
     * Look up the history record based on the file's checksum. If it indicates
     * the file was partially processed, reset the read position to where it
     * left off previously.
     *
     * @param fileToTail
     * @return the files processing status, COMPLETE, INCOMPLETE, NOT_STARTED
     * @throws Exception
     */
    private FileReadStatus setupFileReader(File fileToTail, long checksum,
            QiaoFileEntry historyEntry) throws Exception
    {

        // check position cache for last read offset
        FileReadingPositionCache _position = getPoistionFromCache(checksum,
                fileToTail.getAbsolutePath());
        FileReadingPositionCache.FileReadState fstate = _position
                .getReadState();

        FileReadStatus status;
        if (historyEntry != null)
        {
            // if history cache entry differs from position cache, take the
            // larger value
            long pos = historyEntry.getOffset();
            if (pos > fstate.position
                    && historyEntry.getTimeProcessed() > fstate.timestamp)
            {
                fstate.position = pos;
                fstate.timestamp = historyEntry.getTimeProcessed();
            }

            status = FileReadStatus.INCOMPLETE;
        }
        else if (fstate.timestamp == 0)
        {
            status = FileReadStatus.NOT_STARTED;
        }
        else
        {
            status = FileReadStatus.INCOMPLETE;
        }

        _position.save();

        // prepare the file reader to get ready for this file
        fileReader.prepare(fileToTail, _position);

        return status;
    }


    private void tryAcquireLock(long checksum)
            throws IOException, InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("try acquiring access lock for file with checksum="
                    + checksum);

        while (fileLockManager.containsFileLock(checksum) && running.get())
            CommonUtils.sleepButInterruptable(100);

        if (!running.get())
            throw new InterruptedException("interrupted");

        if (logger.isDebugEnabled())
            logger.debug("access lock acquired");
    }


    /**
     * Find a file matching with the defined pattern from the source directory.
     *
     * @return
     * @throws IOException
     */
    private Path getNextFile() throws IOException
    {
        if (matchedFileSet.size() == 0)
        {
            finder.reset();
            Files.walkFileTree(srcDirPath, finder);
            List<Path> files = finder.getMatchedFiles();
            for (Path f : files)
            {
                if (logger.isDebugEnabled())
                    logger.debug("found " + f.toString());
                matchedFileSet.add(f);
            }
        }

        if (matchedFileSet.size() > 0)
        {
            Path ans = null;
            Iterator<Path> it = matchedFileSet.iterator();
            if (it.hasNext())
            {
                ans = it.next();
                it.remove();
            }

            return ans;
        }

        return null;
    }


    private void quarantine(Path file, long checksum)
    {
        try
        {
            if (checksum == 0)
                checksum = CommonUtils.checksumOptionalylUseFileLength(
                        file.toFile(), checksumByteLength);

            quarantineFileHandler.moveFileToQuarantineDirIfExists(file,
                    checksum);
        }
        catch (InterruptedException e)
        {
        }
        catch (IOException e)
        {
            logger.warn(e.getClass().getName() + ": " + e.getMessage());
        }
    }


    @Override
    public void setStatsCollector(StatsCollector statsCollector)
    {
        this.statsCollector = statsCollector;
    }


    @Override
    public void setStatsCalculator(StatsCalculator statsCalculator)
    {
        this.statsCalculator = statsCalculator;
    }


    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            logger.info("suspend " + this.getClass().getSimpleName());

            running.set(false);
            fileReader.stop();
            fileReader.awaitTermination(); // wait for position committed before
                                           // close
            CommonUtils.shutdownAndAwaitTermination(executor, 3,
                    TimeUnit.SECONDS);

            status = InjectorStatus.SUSPENDED;
        }
        else
            logger.warn("Nothing to do - already suspended");
    }


    @Override
    public void resume()
    {
        if (isSuspended.compareAndSet(true, false))
        {
            running.set(true);

            try
            {
                copyNewOrUpdatedRecordsFromDependentCache();
                fileReader.start();

                executor = CommonUtils
                        .createSingleThreadExecutor("FileInjector");
                executor.execute(this);

                status = InjectorStatus.ACTIVE;
                logger.info(this.getClass().getSimpleName() + " resumed");

            }
            catch (Exception e)
            {
                logger.error(
                        "failed to resume the opration => " + e.getMessage(),
                        e);
            }
        }
        else
            logger.warn("Nothing to do - not in suspend mode.");

    }


    @Override
    public void shutdown()
    {
        if (running.compareAndSet(true, false))
        {
            logger.info("shutting down " + this.getClass().getSimpleName());

            fileReader.stop();
            fileReader.awaitTermination(); // wait for position committed before
                                           // close
            CommonUtils.shutdownAndAwaitTermination(executor, 3,
                    TimeUnit.SECONDS);

            fileReadPosition.close();

            status = InjectorStatus.INACTIVE;
        }
    }


    @Override
    public boolean isRunning()
    {
        return running.get() && !executor.isTerminated()
                && (status == InjectorStatus.ACTIVE);
    }


    @ManagedAttribute
    @Override
    public String getId()
    {
        return id;
    }


    @Override
    public void setId(String id)
    {
        this.id = id;

    }


    @Override
    public void setFunnelId(String funnelId)
    {
        this.funnelId = funnelId;
    }


    @Override
    public void setDataPipe(IDataPipe dataPipe)
    {
        this.dataPipe = dataPipe;
    }


    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher applicationEventPublisher)
    {
        this.applicationEventPublisher = applicationEventPublisher;
    }


    public String getSrcDir()
    {
        return srcDir;
    }


    /**
     * The source directory for the injector to locate files.
     *
     * @param srcDir
     */
    public void setSrcDir(String srcDir)
    {
        this.srcDir = srcDir;
    }


    /**
     * A global pattern to be matched against the string representation of a
     * file's name. For example, "adserver.log.1.*".
     *
     * @param filePattern
     */
    public void setFilePattern(String filePattern)
    {
        this.filePattern = filePattern;
    }


    /**
     * Number of milliseconds to wait until next check for file's availability
     * when directory is empty or none matches.
     *
     * @param fileCheckDelayMillis
     */
    public void setFileCheckDelayMillis(long fileCheckDelayMillis)
    {
        this.fileCheckDelayMillis = fileCheckDelayMillis;
    }


    /**
     * Defines internal buffer size in bytes. Default is 4096.
     *
     * @param bufSize
     */
    public void setBufSize(int bufSize)
    {
        this.bufSize = bufSize;
    }


    public void setDataHandler(ITailerDataHandler<T> dataHandler)
    {
        this.dataHandler = dataHandler;
    }


    public void setCallback(ICallback callback)
    {
        this.callback = callback;
    }


    public void setStatKeyInputs(String statKeyInputs)
    {
        this.statKeyInputs = statKeyInputs;
    }


    public void setStatKeyFiles(String statKeyFiles)
    {
        this.statKeyFiles = statKeyFiles;
    }


    public void setCounterKeys(Map<String, PubStats> counterKeys)
    {
        this.counterKeys = counterKeys;
    }


    public void setChecksumByteLength(int checksumByteLength)
    {
        this.checksumByteLength = checksumByteLength;
    }


    public void setReadMode(READ_MODE readMode)
    {
        this.readMode = readMode;
    }


    /**
     * Define Read Mode. Valid choices are TEXTBLOCK, BINARY.
     *
     * @param readrMode
     */
    public void setReadMode(String readrMode)
    {
        this.readMode = READ_MODE.valueOf(readrMode);
    }


    @ManagedAttribute
    public String getFilePattern()
    {
        return filePattern;
    }


    @ManagedAttribute
    public long getFileCheckDelayMillis()
    {
        return fileCheckDelayMillis;
    }


    @ManagedAttribute
    public String getReadMode()
    {
        return readMode.name();
    }


    @ManagedAttribute
    public int getBufSize()
    {
        return bufSize;
    }


    @ManagedAttribute
    public AtomicBoolean getRunning()
    {
        return running;
    }


    @ManagedAttribute
    public String getFunnelId()
    {
        return funnelId;
    }


    @ManagedAttribute
    public int getChecksumByteLength()
    {
        return checksumByteLength;
    }


    @ManagedAttribute
    public String getCurrentFile()
    {
        if (currentFile != null)
            return currentFile;

        return "";
    }


    @ManagedAttribute
    public long getCurrentReadPosition()
    {
        if (currentFile != null)
            return fileReader.getReadPosition();

        return -1;
    }


    @ManagedAttribute
    public long getCurrentFileChecksum()
    {
        return currentReadFileChecksum;
    }


    @ManagedOperation
    public String getCurrentFileModTimeAndReadTime()
    {
        if (currentFile != null)
            return fileReader.getCurrentFileModTimeAndReadTime();

        return "Not available";

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


    @ManagedMetric
    public long getNumInputsAtInterval()
    {
        return numInputs.get();
    }


    @ManagedMetric
    public long getNumFilesAtInterval()
    {
        return numFiles.get();
    }


    @Override
    public boolean isSuspended()
    {
        return isSuspended.get();
    }


    public void setMaxFilesToFind(int maxFilesToFind)
    {
        this.maxFilesToFind = maxFilesToFind;
    }


    public void setPositionCache(PositionCache positionCache)
    {
        this.positionCache = positionCache;
    }


    @Override
    public void setPositionCacheDependency(PositionCache initPositionCacheFrom)
    {
        this.initPositionCacheFrom = initPositionCacheFrom;
    }


    @Override
    public PositionCache getPositionCache()
    {
        return this.positionCache;
    }


    @Override
    public void copyNewOrUpdatedRecordsFromDependentCache() throws Exception
    {

        if (initPositionCacheFrom != null
                && initPositionCacheFrom != positionCache)
        {
            int count = positionCache.copyNewRecordsFrom(initPositionCacheFrom);
            logger.info("total records copied: " + count);
        }
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


    public void setCacheDiskReapingIntervalSecs(
            int cacheDiskReapingIntervalSecs)
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


    @Override
    public void setAgentId(String agentId)
    {
        this.agentId = agentId;
    }
}
