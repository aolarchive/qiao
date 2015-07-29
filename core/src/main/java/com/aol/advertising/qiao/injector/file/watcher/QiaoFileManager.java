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

package com.aol.advertising.qiao.injector.file.watcher;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.injector.file.DoneFileHandler;
import com.aol.advertising.qiao.injector.file.watcher.FileOperationEvent.EVENT_TYPE;
import com.aol.advertising.qiao.management.FileReadingPositionCache;
import com.aol.advertising.qiao.management.ISuspendable;
import com.aol.advertising.qiao.management.QiaoFileBookKeeper;
import com.aol.advertising.qiao.management.QiaoFileEntry;
import com.aol.advertising.qiao.management.QuarantineFileHandler;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.FileFinder;

/**
 * Iterate through files whose names match a predefined pattern at the specific
 * source directory. If a file has been completely injected, move the file to
 * done directory.
 *
 */
@ManagedResource
public class QiaoFileManager implements Runnable, ISuspendable,
        IFileEventListener
{

    private static final int DEFAULT_QUEUE_CAPACITY = 1024;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // input properties
    private String srcDir; // source file directory
    private String donefilePattern; // file name matching pattern
    private long fileCheckDelayMillis = 1000; // wait time for files to be available
    private int maxFilesToFind = -1;
    private int checksumByteLength = 2048;
    private QiaoFileBookKeeper bookKeeper;
    private DoneFileHandler doneFileHandler;
    private QuarantineFileHandler quarantineFileHandler;

    private Path srcDirPath;
    private FileFinder finder;
    private ExecutorService executor;
    private FileReadingPositionCache fileReadPosition;

    private AtomicBoolean running = new AtomicBoolean(false);
    private AtomicBoolean isSuspended = new AtomicBoolean(false);

    private Set<Path> matchedFileSet = new HashSet<Path>();

    //
    private String candidateFilesPatternForRename;
    private PathMatcher pathMatcher;
    //
    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    private BlockingQueue<FileOperationEvent> queue;
    //
    private String filesPatternForRenameOnInit;


    public void init() throws Exception
    {
        _validate();

        if (queueCapacity <= 0)
            queueCapacity = DEFAULT_QUEUE_CAPACITY;

        queue = new ArrayBlockingQueue<FileOperationEvent>(queueCapacity);

        srcDirPath = Paths.get(srcDir);

        if (filesPatternForRenameOnInit == null)
            filesPatternForRenameOnInit = donefilePattern;

        _setupFileFinder();

        _setupDoneFileHandler();

        _setupFileRenameMatcher();

        _setupQuarantineFileHandler();

        logger.info(this.getClass().getSimpleName() + " initialized");

    }


    private void _validate()
    {

        if (srcDir == null)
            throw new ConfigurationException("srcDir not defined");

        if (donefilePattern == null)
            throw new ConfigurationException("filePattern not defined");

        if (bookKeeper == null)
            throw new ConfigurationException("bookKeeper not set");

        if (doneFileHandler == null)
            throw new ConfigurationException("doneFileHandler not set");

        if (candidateFilesPatternForRename == null)
            throw new ConfigurationException(
                    "candidateFilesPatternForRename not defined");

        if (quarantineFileHandler == null)
            throw new ConfigurationException("quarantineFileHandler not set");

    }


    private void _setupFileFinder()
    {
        finder = new FileFinder(donefilePattern);
        finder.setMaxFiles(maxFilesToFind);
    }


    private void _setupDoneFileHandler()
    {
        doneFileHandler.setSrcDir(srcDir);
        doneFileHandler.init();
    }


    private void _setupQuarantineFileHandler()
    {
        quarantineFileHandler.setSrcDir(srcDir);
        quarantineFileHandler.init();
    }


    private void _setupFileRenameMatcher() throws Exception
    {
        FileSystem fileSystem = FileSystems.getDefault();
        pathMatcher = fileSystem.getPathMatcher("glob:**/"
                + candidateFilesPatternForRename);
    }


    private void preStart()
    {
        FileFinder finder = new FileFinder(filesPatternForRenameOnInit);
        finder.setMaxFiles(-1);

        finder.reset();
        try
        {
            Files.walkFileTree(srcDirPath, finder);

            List<Path> files = finder.getMatchedFiles();
            for (Path f : files)
            {
                long checksum = CommonUtils.checksumOptionalylUseFileLength(
                        f.toFile(), checksumByteLength);

                if (!doneFileHandler.nameContainsChecksum(f, checksum))
                {
                    Path new_path = doneFileHandler
                            .renameFileToIncludeChecksum(f, checksum);
                    logger.info("renamed " + f.toString() + " to "
                            + new_path.toString());
                }
            }
        }
        catch (IOException e)
        {
            logger.warn("error in preStart: " + e.getMessage(), e);
        }
        catch (InterruptedException e)
        {
        }

    }


    public void start() throws Exception
    {
        if (running.compareAndSet(false, true))
        {
            preStart();

            executor = CommonUtils.createSingleThreadExecutor("FileManager");
            executor.execute(this);

            logger.info(this.getClass().getSimpleName() + " started");
        }
    }


    @Override
    public void run()
    {

        while (running.get())
        {
            try
            {
                FileOperationEvent event = getNextEvent();
                if (event != null)
                {
                    switch (event.eventType)
                    {
                        case MOVE_FILE:
                            doneFileHandler.moveFileToDoneDirIfExists(
                                    event.filePath, event.checksum);
                            break;
                        case RENAME_FILE:
                            renameFile(event.filePath, event.newfilePath);
                            break;
                        default:
                            logger.error("invalid event type => "
                                    + event.eventType);
                    }

                    continue;
                }

                Path file = getNextFile();
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

                long checksum = CommonUtils.checksumOptionalylUseFileLength(
                        file.toFile(), this.checksumByteLength);

                if (isFileDone(file, checksum))
                {
                    doneFileHandler.moveFileToDoneDirIfExists(file, checksum);
                }
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("skipped " + file.toString()
                                + " - not done");
                }

            }
            catch (InterruptedException e)
            {
                logger.info("interrupted");
            }
            catch (Exception e)
            {
                logger.error(e.getMessage(), e);
            }
        }

        logger.info(this.getClass().getSimpleName() + " terminated");
    }


    private boolean isFileDone(Path filePath, long checksum)
    {
        QiaoFileEntry entry = bookKeeper.getHistory(checksum);
        if (entry == null)
            return false;

        if (entry.isCompleted())
        {
            logger.info("File " + filePath + " processing is done. metadata="
                    + entry);
            return true;
        }

        // partially processed
        return false;
    }


    private FileOperationEvent getNextEvent()
    {
        return queue.poll();
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
            CommonUtils.sleepQuietly(fileCheckDelayMillis);

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


    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            shutdown();
        }
        else
            logger.warn("Nothing to do - already suspended");
    }


    @Override
    public void resume()
    {
        if (isSuspended.compareAndSet(true, false))
        {

            try
            {
                start();
            }
            catch (Exception e)
            {
                logger.error(
                        "failed to resume the opration => " + e.getMessage(), e);
            }
        }
        else
            logger.warn("Nothing to do - not in suspend mode.");

    }


    public void shutdown()
    {
        if (running.compareAndSet(true, false))
        {
            logger.info("shutting down " + this.getClass().getSimpleName());

            executor.shutdown();
        }
    }


    public boolean isRunning()
    {
        return running.get() && !executor.isTerminated();
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
        this.donefilePattern = filePattern;
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


    public void setChecksumByteLength(int checksumByteLength)
    {
        this.checksumByteLength = checksumByteLength;
    }


    public void setBookKeeper(QiaoFileBookKeeper bookKeeper)
    {
        this.bookKeeper = bookKeeper;
    }


    @ManagedAttribute
    public long getFileCheckDelayMillis()
    {
        return fileCheckDelayMillis;
    }


    @ManagedAttribute
    public AtomicBoolean getRunning()
    {
        return running;
    }


    @ManagedAttribute
    public int getChecksumByteLength()
    {
        return checksumByteLength;
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


    public void setMaxFilesToFind(int maxFilesToFind)
    {
        this.maxFilesToFind = maxFilesToFind;
    }


    public void setDoneFileHandler(DoneFileHandler doneFileHandler)
    {
        this.doneFileHandler = doneFileHandler;
    }


    @Override
    public void onCreate(Path file)
    {
        if (Files.notExists(file))
        {
            logger.info("file " + file + " does not exist");
            return;
        }

        if (pathMatcher.matches(file))
        {
            try
            {
                Path new_path = renameToTmpFilePath(file);

                long checksum = CommonUtils.checksumOptionalylUseFileLength(
                        new_path.toFile(), checksumByteLength);

                Path target_path = file;
                if (!nameContainsChecksum(file, checksum))
                {
                    target_path = resolveNewFilePath(file, checksum);
                }

                FileOperationEvent event = new FileOperationEvent(
                        EVENT_TYPE.RENAME_FILE, new_path, checksum, target_path);
                this.addToQueue(event);

            }
            catch (IOException e)
            {
                logger.error(e.getClass().getName() + ": " + e.getMessage());

            }
            catch (InterruptedException e)
            {
            }
        }
    }


    private Path renameToTmpFilePath(Path sourceFilePath) throws IOException
    {
        String tmp_name = UUID.randomUUID() + "_"
                + sourceFilePath.getFileName().toString();
        Path new_path = sourceFilePath.resolveSibling(tmp_name);
        Files.move(sourceFilePath, new_path, StandardCopyOption.ATOMIC_MOVE);

        return new_path;
    }


    private Path renameFile(Path src, Path target) throws IOException
    {
        if (!target.equals(src))
        {
            logger.info("Rename file from " + src + " to " + target);
            Files.move(src, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        }

        return target;
    }


    private boolean nameContainsChecksum(Path filePath, long checksum)
    {
        String name = filePath.getFileName().toString();
        String cksum = String.format(".%d", checksum);
        if (name.contains(cksum))
            return true;

        return false;
    }


    private Path resolveNewFilePath(Path sourceFilePath, long checksum)
    {
        String fname = sourceFilePath.getFileName().toString() + "." + checksum;
        return sourceFilePath.resolveSibling(fname);
    }


    @Override
    public void onDelete(Path file)
    {
        // do nothing
    }


    @Override
    public void onModify(Path file)
    {
        // do nothing
    }


    public void setCandidateFilesPatternForRename(
            String candidateFilesPatternForRename)
    {
        this.candidateFilesPatternForRename = candidateFilesPatternForRename;
    }


    public String getDonefilePattern()
    {
        return donefilePattern;
    }


    public void setDonefilePattern(String donefilePattern)
    {
        this.donefilePattern = donefilePattern;
    }


    public int getQueueCapacity()
    {
        return queueCapacity;
    }


    public void setQueueCapacity(int queueCapacity)
    {
        this.queueCapacity = queueCapacity;
    }


    /**
     * Add the event to the queue - a blocking operation.
     *
     * @param event
     */
    public void addToQueue(FileOperationEvent event)
    {
        try
        {
            queue.put(event);
        }
        catch (InterruptedException e)
        {
        }
    }


    /**
     * Add the event to the queue - a non-blocking operation.
     *
     * @param event
     */
    public boolean addToQueueIfNotFull(FileOperationEvent event)
    {
        return queue.offer(event);

    }


    public DoneFileHandler getDoneFileHandler()
    {
        return doneFileHandler;
    }


    public void setFilesPatternForRenameOnInit(
            String filesPatternForRenameOnInit)
    {
        this.filesPatternForRenameOnInit = filesPatternForRenameOnInit;
    }


    public void setQuarantineFileHandler(
            QuarantineFileHandler quarantineFileHandler)
    {
        this.quarantineFileHandler = quarantineFileHandler;
    }
}
