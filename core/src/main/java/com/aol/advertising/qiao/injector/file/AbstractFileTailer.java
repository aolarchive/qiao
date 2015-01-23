/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2013 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   AbstractFileTailer.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.exception.InsufficientFileLengthException;
import com.aol.advertising.qiao.exception.QiaoOperationException;
import com.aol.advertising.qiao.injector.file.watcher.FileWatchService;
import com.aol.advertising.qiao.injector.file.watcher.IFileEventListener;
import com.aol.advertising.qiao.management.FileLockManager;
import com.aol.advertising.qiao.management.FileReadingPositionCache;
import com.aol.advertising.qiao.management.QiaoFileEntry;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.ContextUtils;
import com.aol.advertising.qiao.util.IntervalMetric;
import com.sleepycat.je.Transaction;

/**
 * This abstract class that reads and follows a file in a way similar to Unix
 * 'tail -F' command.
 * 
 * @param <T>
 *            data format of the file: String or ByteBuffer
 */
public abstract class AbstractFileTailer<T> implements ITailer<T>,
        IFileEventListener
{
    protected static final String RAF_MODE = "r";

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final byte[] inbuf; // Buffer on top of RandomAccessFile
    protected final File tailedFile; // The file which will be tailed   
    protected final long delayMillis; // The amount of time to wait for the file to be updated
    protected final int bufSize; // buffer size
    protected FileReadingPositionCache _position; // current read position from the beginning of the file
    protected final ITailerDataHandler<T> dataHandler; // user-supplied data handling routine
    protected volatile boolean running = true; // The tailer will run as long as this value is true
    protected long fileCheckDelayMillis = 3000;

    protected AtomicLong numInputs;
    protected AtomicLong numFiles;
    protected ICallback callback;
    protected CRC32 _crc = new CRC32(); // working variable
    protected int checksumByteLength = 2048;
    protected List<IFileOperationListener> operationListeners = new ArrayList<IFileOperationListener>();
    protected long currentReadFileChecksum;
    protected boolean autoCommitFilePosition = false;

    protected IntervalMetric fpStats;

    protected volatile boolean safeToShutdown = false;

    protected FileLockManager fileLockManager;
    protected AtomicBoolean newFileDetected = new AtomicBoolean(false);


    public AbstractFileTailer(File file, long delayMillis, int bufSize,
            FileReadingPositionCache position, ITailerDataHandler<T> dataHandler)
    {
        this.tailedFile = file;
        this.delayMillis = delayMillis;
        this.bufSize = bufSize;
        this.inbuf = new byte[bufSize];
        this._position = position;
        this.dataHandler = dataHandler;
    }


    @Override
    public void init() throws Exception
    {
        _validate();

        logger.info("register to file watcher..."); //TODO: delete
        FileWatchService fileWatcher = ContextUtils
                .getBean(FileWatchService.class);
        fileWatcher.registerListener(this);

        if (dataHandler != null)
        {
            FileReadingPositionCache.FileReadState new_state = dataHandler
                    .init(this);
            if (new_state != null)
            {
                setFileReadState(new_state);
            }
        }
    }


    @Override
    public void start()
    {
        running = true;
    }


    private void _validate()
    {

        if (numInputs == null)
            throw new ConfigurationException("numInputs not set");

        if (numFiles == null)
            throw new ConfigurationException("numFiles not set");

        if (callback == null)
            throw new ConfigurationException("callback not set");
    }


    /**
     * Follows changes in the file, calling the callback's handle method for
     * each new line or new block.
     * 
     * <pre>
     * - Open the file
     * - Compute the checksum of the file
     * - Set read position
     * - Lock the file using the checksum
     * - Process the file until file is done and a new file is created 
     * - Notify listeners on complete
     * - Unlock the file
     * </pre>
     */
    @Override
    public void run()
    {
        long ts_start = System.currentTimeMillis();

        boolean file_rotated_or_truncated = false;
        RandomAccessFile reader = null;
        String filename = tailedFile.getAbsolutePath();
        try
        {
            reader = prepareFileToRead(tailedFile);

            // get last read position via checksum. 
            FileReadingPositionCache.FileReadState fstate = _position
                    .getReadState();
            logger.info(String
                    .format("LAST PROCESSING STATUS: file=%s, timestamp=%s, read_position=%d, checksum=%d",
                            filename, fstate.getFriendlyTimestamp(),
                            fstate.position, fstate.checksum));

            logger.info(">continue processing from last read: "
                    + _position.toString());

            numFiles.set(1);
            while (running)
            {
                if (file_rotated_or_truncated)
                    ts_start = System.currentTimeMillis();

                file_rotated_or_truncated = process(reader, delayMillis,
                        file_rotated_or_truncated);
                if (file_rotated_or_truncated)
                {
                    IOUtils.closeQuietly(reader);

                    fstate = _position.getReadState();
                    long mod_time = tailedFile.lastModified();
                    notifyListenerOnComplete(new QiaoFileEntry(
                            tailedFile.getAbsolutePath(), mod_time,
                            fstate.checksum, fstate.timestamp, fstate.position,
                            true));

                    long dur = System.currentTimeMillis() - ts_start;
                    fpStats.update(dur);

                    _position.remove();
                    if (logger.isDebugEnabled())
                        logger.debug("removed " + currentReadFileChecksum
                                + " from positionCache");

                    fileLockManager.removeFileLock(currentReadFileChecksum); // delete lock
                    logger.info("File " + tailedFile.getAbsolutePath()
                            + " rotated");

                    currentReadFileChecksum = 0;

                    // --- another file ---
                    reader = prepareFileToRead(tailedFile);

                    numFiles.incrementAndGet(); // incr file count
                    if (dataHandler != null)
                        dataHandler.fileRotated();

                }
                else
                {
                    CommonUtils.sleepQuietly(delayMillis);
                }
            }
        }
        catch (InterruptedException e)
        {
        }
        catch (QiaoOperationException e)
        {
            logger.error(e.getMessage(), e);
        }
        catch (Throwable t)
        {
            logger.error(t.getMessage(), t);
        }
        finally
        {
            if (dataHandler != null)
                dataHandler.close();

            IOUtils.closeQuietly(reader);
            if (currentReadFileChecksum != 0)
                fileLockManager.removeFileLock(currentReadFileChecksum);
        }

        logger.info(this.getClass().getSimpleName() + " terminated");

    }


    /**
     * Open the file to read, notify listeners, add checksum to position cache,
     * and lock the file.
     * 
     * @param tailedFile
     * @return RandomAccessFile for tailedFile, or null if interrupted.
     * @throws QiaoOperationException
     */
    private RandomAccessFile prepareFileToRead(File tailedFile)
            throws QiaoOperationException, InterruptedException
    {
        String filename = tailedFile.getAbsolutePath();
        RandomAccessFile raf = openFile(tailedFile);
        notifyListenerOnOpen(filename); // let listeners know

        try
        {
            raf = getChecksumAndResetPositionKey(raf);
        }
        catch (QiaoOperationException e)
        {
            String s;
            logger.error(s = "Unable to get checksum for file " + filename);
            throw new QiaoOperationException(s, e);
        }

        currentReadFileChecksum = _position.getReadState().checksum;
        fileLockManager.addFileLock(currentReadFileChecksum); // add lock

        return raf;
    }


    protected RandomAccessFile getChecksumAndResetPositionKey(
            RandomAccessFile raf) throws QiaoOperationException,
            InterruptedException
    {
        while (running)
        {
            try
            {
                long crc = checksum(raf);
                _position.setKey(crc); // set checksum as the key

                logger.info(">> New file's checksum=" + crc);

                return raf;
            }
            catch (IOException e)
            {
                logger.error(e.getClass().getName() + ": " + e.getMessage());
                throw new QiaoOperationException("Unable to get checksum", e);
            }
            catch (InsufficientFileLengthException e)
            {
                // not enough data

                if (this.newFileDetected.compareAndSet(true, false))
                {
                    logger.info("=> switch to process the new file");
                    IOUtils.closeQuietly(raf);

                    raf = openFile(tailedFile);
                    notifyListenerOnOpen(tailedFile.getAbsolutePath()); // let listeners know
                    continue;
                }
                /*
                // in case someone deletes the file
                long len = tailedFile.length();
                if (len > checksumByteLength)
                {
                    logger.warn("Discrepancy between current file descriptor and current file => close and re-open");
                    IOUtils.closeQuietly(raf);

                    raf = openFile(tailedFile);
                    notifyListenerOnOpen(tailedFile.getAbsolutePath()); // let listeners know
                    continue;
                }*/
            }

            CommonUtils.sleepQuietly(fileCheckDelayMillis);
        }

        return raf;
    }


    protected RandomAccessFile openFileQuietly(File file)
    {

        RandomAccessFile reader = null;
        while (running && reader == null)
        {
            try
            {
                reader = new RandomAccessFile(file, RAF_MODE);
            }
            catch (FileNotFoundException e)
            {
            }

            if (reader != null)
                return reader;

            boolean timed_out = CommonUtils.sleepQuietly(fileCheckDelayMillis);
            if (!timed_out) // interrupted
                break;
        }

        return null;
    }


    /*
     * Open the file. Will wait until file is available.
     */
    protected RandomAccessFile openFile(File file) throws InterruptedException
    {
        RandomAccessFile reader = null;
        while (running && reader == null)
        {
            try
            {
                reader = new RandomAccessFile(file, RAF_MODE);
            }
            catch (FileNotFoundException e)
            {
                if (dataHandler != null)
                    dataHandler.fileNotFound();
            }

            if (reader != null)
                return reader;

            boolean timed_out = CommonUtils.sleepQuietly(fileCheckDelayMillis);
            if (!timed_out) // interrupted
                break;
        }

        throw new InterruptedException("interrupted");

    }


    protected void savePositionAndInvokeCallbackAutoCommit(T data, long pos)
    {

        try
        {

            int pos_adj = invokeCallback(data);
            if (pos_adj != 0)
                pos += pos_adj; // adjust position to end of last record.  in case of Qiao restarts, we can position the file to the proper record boundary.

            _position.set(pos);

            numInputs.incrementAndGet();

        }
        catch (Exception e)
        {
            logger.info("savePositionAndInvokeCallback error: "
                    + e.getClass().getSimpleName() + ":" + e.getMessage(), e);
        }
    }


    // faster than auto-commit
    protected void savePositionAndInvokeCallback(T data, long pos)
    {

        if (autoCommitFilePosition)
        {
            savePositionAndInvokeCallbackAutoCommit(data, pos);
            return;
        }

        // -------------------------------

        boolean committed = false;
        Transaction trx = null;
        try
        {
            trx = _position.beginTransaction(); // transactional starts --->
            if (trx == null)
                logger.warn("unable to acquire a transaction. trx is null");

            int pos_adj = invokeCallback(data);
            if (pos_adj != 0)
                pos += pos_adj; // adjust position to end of last record.  in case of Qiao restarts, we can position the file to the proper record boundary.

            boolean saved = setPosition(pos);

            numInputs.incrementAndGet();

            if (saved)
            {
                _position.commit(); // <--- transaction end
                committed = true;
            }
        }
        catch (Throwable e)
        {
            logger.warn("savePositionAndInvokeCallback>"
                    + e.getClass().getSimpleName() + ":" + e.getMessage(), e);
        }
        finally
        {
            if (!committed && (trx != null))
            {
                abortTransactiolnSliently();
                logger.info("transaction aborted");
            }
        }
    }


    private void abortTransactiolnSliently()
    {
        try
        {
            _position.abortTransaction();
        }
        catch (Throwable t)
        {
            if (logger.isDebugEnabled())
                logger.debug("abortTransaction>" + t.getClass().getSimpleName()
                        + ":" + t.getMessage());

        }
    }


    private boolean setPosition(long pos)
    {
        try
        {
            _position.set(pos);
            return true;
        }
        catch (Throwable e)
        {
            logger.warn("position.set failed: " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        }

        return false;
    }


    /*
     * Invoke callback. Typically this results in dropping data to the internal
     * queue. It returns position adjustment to the original read position to
     * identify the end of last logical record processed from the data block.
     */
    protected int invokeCallback(T data) throws Exception
    {
        if (dataHandler != null)
        {
            Iterator< ? > iter;
            try
            {
                iter = dataHandler.onData(data);
                if (iter != null)
                {
                    while (iter.hasNext())
                        callback.receive(iter.next());
                }
            }
            catch (Exception e)
            {
                logger.error(
                        "data handler throwed exception: " + e.getMessage(), e);
                throw e;
            }

            return dataHandler.getPositionAdjustment();
        }
        else
        {
            callback.receive(data);
            return 0;
        }
    }


    /**
     * Allows the tailer to complete its current loop and return.
     */
    @Override
    public void stop()
    {
        this.running = false;
    }


    @Override
    public void setFileReadState(
            FileReadingPositionCache.FileReadState readState)
    {
        _position.set(readState.position, readState.timestamp,
                readState.checksum);
    }


    @Override
    public long getReadPosition()
    {
        return _position.get();
    }


    /**
     * Return the file.
     * 
     * @return the file
     */
    @Override
    public File getFile()
    {
        return tailedFile;
    }


    @Override
    public FileReadingPositionCache.FileReadState getFileReadState()
    {
        return _position.getReadState();
    }


    /**
     * Return the delay in milliseconds.
     * 
     * @return the delay in milliseconds.
     */
    public long getDelay()
    {
        return delayMillis;
    }


    public int getBufSize()
    {
        return bufSize;
    }


    public void setNumInputs(AtomicLong numInput)
    {
        this.numInputs = numInput;
    }


    abstract protected boolean process(RandomAccessFile reader,
            long pullDelayMillis, boolean fileRotated)
            throws InterruptedException;


    public String getCurrentFileModTimeAndReadTime()
    {
        // take snapshot 
        long mod_time = tailedFile.lastModified();
        long read_time = _position.getLastTimestamp();
        long file_crc = getCurrentFileChecksum();
        long read_crc = _position.getReadState().checksum;

        return String
                .format("File (crc=%d) lastModTime=%d, file (crc=%d) lastReadTime=%d, (ModTime > ReadTime? %b)",
                        file_crc, mod_time, read_crc, read_time,
                        mod_time > read_time);
    }


    public long getCurrentFileChecksum()
    {
        try
        {
            return checksum(tailedFile);
        }
        catch (IOException | InterruptedException e)
        {
            logger.warn("unable to get checksum");
            return 0;
        }
    }


    public long getCurrentReadingFileChecksum()
    {
        return this.currentReadFileChecksum;
    }


    protected void logFileTruncation(long fileSize, long readPosition)
    {
        final String fmt = "File truncated (file_len=%d, read_pos=%d). Process from the beginning...";
        logger.info(String.format(fmt, fileSize, readPosition));
    }


    public void setCallback(ICallback callback)
    {
        this.callback = callback;
    }


    public void setNumFiles(AtomicLong numFiles)
    {
        this.numFiles = numFiles;
    }


    protected long checksum(RandomAccessFile raFile) throws IOException,
            InsufficientFileLengthException
    {
        long pos = raFile.getFilePointer();
        try
        {
            byte[] buffer = new byte[checksumByteLength];
            raFile.seek(0);
            int n = raFile.read(buffer);
            if (n < checksumByteLength)
            {
                String s;
                logger.warn(s = ("not enough data for checksum: current file size=" + n));
                throw new InsufficientFileLengthException(s);
            }

            synchronized (_crc)
            {
                _crc.reset();
                _crc.update(buffer);

                return _crc.getValue();
            }
        }
        finally
        {
            raFile.seek(pos);
        }

    }


    protected long checksum(File file) throws IOException, InterruptedException
    {
        RandomAccessFile raf = openFile(file);

        try
        {
            long v = checksum(raf);
            return v;
        }
        finally
        {
            IOUtils.closeQuietly(raf);
        }

    }


    // Wait quietly for file to be available and take a checksum.
    protected long checksumWaitQuietlyIfFileNotExist(File file)
            throws IOException, InterruptedException
    {
        RandomAccessFile raf = openFileQuietly(file);
        if (raf == null)
            throw new InterruptedException(); // was interrupted

        try
        {
            long v = checksum(raf);
            return v;
        }
        finally
        {
            IOUtils.closeQuietly(raf);
        }

    }


    protected boolean hasNewFile(File tailedFile, long checksum)
            throws InterruptedException
    {

        try
        {
            long crc = checksumWaitQuietlyIfFileNotExist(tailedFile);
            return (crc != checksum);
        }
        catch (InsufficientFileLengthException e)
        {
            logger.info("new file is not long enough for checksum");
            return true;
        }
        catch (IOException e)
        {
            logger.error(e.getClass().getName() + ": " + e.getMessage());
            return false;
        }
    }


    protected boolean isSameFile(File tailedFile, long checksum)
            throws InterruptedException
    {

        try
        {
            long crc = checksum(tailedFile);
            return (crc == checksum);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return false;
        }
    }


    public void registerListener(IFileOperationListener listener)
    {
        this.operationListeners.add(listener);
    }


    protected void notifyListenerOnOpen(String filename)
    {

        for (IFileOperationListener listener : operationListeners)
        {
            listener.onOpen(filename);
        }
    }


    protected void notifyListenerOnComplete(QiaoFileEntry file)
    {

        for (IFileOperationListener listener : operationListeners)
        {
            listener.onComplete(file);
        }

    }


    public IntervalMetric getFpStats()
    {
        return fpStats;
    }


    public void setChecksumByteLength(int checksumByteLength)
    {
        this.checksumByteLength = checksumByteLength;
    }


    public void setFileCheckDelayMillis(long fileCheckDelayMillis)
    {
        this.fileCheckDelayMillis = fileCheckDelayMillis;
    }


    public void awaitTermination()
    {
        while (!safeToShutdown)
            CommonUtils.sleepQuietly(10);

    }


    public void setFileLockManager(FileLockManager lockManager)
    {
        this.fileLockManager = lockManager;
    }


    public void setFpStats(IntervalMetric fpStats)
    {
        this.fpStats = fpStats;
    }


    public void setAutoCommitFilePosition(boolean autoCommitFilePosition)
    {
        this.autoCommitFilePosition = autoCommitFilePosition;
    }


    @Override
    public void onCreate(Path file)
    {
        if (file.toFile().equals(this.tailedFile))
        {
            // has a new file
            logger.info("new file created => " + file.toAbsolutePath());
            newFileDetected.set(true);
        }
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
}
