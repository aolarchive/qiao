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

package com.aol.advertising.qiao.injector.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.management.FileReadingPositionCache;
import com.aol.advertising.qiao.management.QiaoFileEntry;
import com.aol.advertising.qiao.util.CommonUtils;
import com.sleepycat.je.Transaction;

/**
 * An abstract class that reads a file.
 *
 * @param <T>
 *            data format of the file: String or ByteBuffer
 */
public abstract class AbstractFileReader<T> implements IFileReader<T>
{
    protected static final String RAF_MODE = "r";

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final byte[] inbuf; // Buffer on top of RandomAccessFile
    protected File tailedFile; // The file which will be tailed
    protected final int bufSize; // buffer size
    protected FileReadingPositionCache readPosition; // current read position from the beginning of the file
    protected final ITailerDataHandler<T> dataHandler; // user-supplied data handling routine
    protected volatile boolean running = true; // The tailer will run as long as this value is true
    protected long fileCheckDelayMillis = 1000;
    protected boolean autoCommitFilePosition = false;

    protected AtomicLong numInputs;
    protected AtomicLong numFiles;
    protected ICallback callback;
    protected CRC32 _crc = new CRC32(); // working variable
    protected int checksumByteLength = 2048;
    protected List<IFileOperationListener> operationListeners = new ArrayList<IFileOperationListener>();
    protected long currentReadFileChecksum;
    protected volatile boolean safeToShutdown = false;


    public AbstractFileReader(int bufSize, ITailerDataHandler<T> dataHandler)
    {
        this.bufSize = bufSize;
        this.inbuf = new byte[bufSize];
        this.dataHandler = dataHandler;
    }


    @Override
    public void init() throws Exception
    {
        _validate();
    }


    @Override
    public void start() throws Exception
    {
        running = true;
    }


    public void prepare(File file, FileReadingPositionCache readPosition)
            throws Exception
    {
        this.tailedFile = file;
        this.readPosition = readPosition;

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
     */
    public boolean execute()
    {

        RandomAccessFile raf = null;
        try
        {
            raf = openFileQuietly(tailedFile);
            if (raf == null)
                return false; // interrupted

            notifyListenerOnOpen(tailedFile.getAbsolutePath());

            logger.info("> Start processing from: " + readPosition.toString());

            this.currentReadFileChecksum = readPosition.getReadState().checksum;

            process(raf);
            IOUtils.closeQuietly(raf);
            raf = null;

            FileReadingPositionCache.FileReadState fstate = readPosition
                    .getReadState();
            long mod_time = tailedFile.lastModified();
            notifyListenerOnComplete(new QiaoFileEntry(
                    tailedFile.getAbsolutePath(), mod_time, fstate.checksum,
                    fstate.timestamp, fstate.position, true));

            numFiles.incrementAndGet(); // incr file count

            return true;
        }
        catch (InterruptedException e)
        {
        }
        catch (Throwable t)
        {
            logger.error(t.getMessage(), t);
        }
        finally
        {
            if (raf != null)
            {
                FileReadingPositionCache.FileReadState fstate = readPosition
                        .getReadState();
                logger.info(">last position=" + fstate.position
                        + " for file with checksum " + fstate.checksum);

                numFiles.incrementAndGet(); // incr file count
            }

            if (dataHandler != null)
                dataHandler.close();

            if (raf != null)
                IOUtils.closeQuietly(raf);
        }

        return false;
    }


    protected boolean setChecksum(RandomAccessFile raf)
    {
        boolean done = false;

        for (int i = 0; !done && i < 3; i++)
        {
            try
            {
                long crc = checksum(raf);

                FileReadingPositionCache.FileReadState state = readPosition
                        .getReadState();
                logger.info(">> New file's checksum=" + crc);
                state.checksum = crc;
                readPosition.save();
                done = true;

                break;
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            catch (IllegalStateException e)
            {
            }

            CommonUtils.sleepQuietly(10);
        }

        return done;
    }


    protected RandomAccessFile openFileQuietly(File file)
    {
        try
        {
            return openFile(file);
        }
        catch (InterruptedException e)
        {
        }

        return null;
    }


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

            readPosition.set(pos);

            numInputs.incrementAndGet();

        }
        catch (Exception e)
        {
            logger.info("savePositionAndInvokeCallback error: "
                    + e.getClass().getSimpleName() + ":" + e.getMessage(), e);
        }
    }


    protected void savePositionAndInvokeCallback(T data, long pos)
    {
        if (autoCommitFilePosition)
        {
            savePositionAndInvokeCallbackAutoCommit(data, pos);
            return;
        }

        // -------------------------

        boolean committed = false;
        Transaction trx = null;
        try
        {
            trx = readPosition.beginTransaction(); // transactional starts --->
            if (trx == null)
                logger.warn("unable to acquire a transaction. trx is null");

            int pos_adj = invokeCallback(data);
            if (pos_adj != 0)
                pos += pos_adj; // adjust position to end of last record.  in case of Qiao restarts, we can position the file to the proper record boundary.

            //System.out.println("pos adjustment: " + pos_adj); // trace

            boolean saved = setPosition(pos);

            numInputs.incrementAndGet();

            if (saved)
            {
                readPosition.commit(); // <--- transaction end
                committed = true;
            }
        }
        catch (Exception e)
        {
            logger.warn("savePositionAndInvokeCallback>"
                    + e.getClass().getSimpleName() + ":" + e.getMessage());

        }
        finally
        {
            if (!committed && (trx != null))
            {
                logger.info("abort transaction");
                abortTransactiolnSliently();
            }
        }

    }


    private void abortTransactiolnSliently()
    {
        try
        {
            readPosition.abortTransaction();
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
            readPosition.set(pos);
            return true;
        }
        catch (Throwable e)
        {
            logger.warn("position.set failed: " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());

        }

        return false;
    }


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
        readPosition.set(readState.position, readState.timestamp,
                readState.checksum);
    }


    @Override
    public long getReadPosition()
    {
        return readPosition.get();
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
        return readPosition.getReadState();
    }


    public int getBufSize()
    {
        return bufSize;
    }


    public void setNumInputs(AtomicLong numInput)
    {
        this.numInputs = numInput;
    }


    abstract protected void process(RandomAccessFile reader)
            throws InterruptedException;


    public String getCurrentFileModTimeAndReadTime()
    {
        // take snapshot
        long mod_time = tailedFile.lastModified();
        long read_time = readPosition.getLastTimestamp();
        long file_crc = getCurrentFileChecksum();
        long read_crc = readPosition.getReadState().checksum;

        return String
                .format("File (crc=%d) lastModTime=%d, file (crc=%d) lastReadTime=%d, (ModTime > ReadTime? %b)",
                        file_crc, mod_time, read_crc, read_time,
                        mod_time > read_time);
    }


    public long getCurrentFileChecksum()
    {
        return readPosition.getReadState().checksum;
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
            IllegalStateException
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
                throw new IllegalStateException(s);
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


    public void setChecksumByteLength(int checksumByteLength)
    {
        this.checksumByteLength = checksumByteLength;
    }


    public void setFileCheckDelayMillis(long fileCheckDelayMillis)
    {
        this.fileCheckDelayMillis = fileCheckDelayMillis;
    }


    public void setTailedFile(File tailedFile)
    {
        this.tailedFile = tailedFile;
    }


    public void setReadPosition(FileReadingPositionCache readPosition)
    {
        this.readPosition = readPosition;
    }


    public void awaitTermination()
    {
        while (!safeToShutdown)
            CommonUtils.sleepQuietly(10);

    }


    public void setAutoCommitFilePosition(boolean autoCommitFilePosition)
    {
        this.autoCommitFilePosition = autoCommitFilePosition;
    }
}
