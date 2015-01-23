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
 * File Name:   TextFileTailer.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector.file;

import java.io.File;
import java.io.RandomAccessFile;

import com.aol.advertising.qiao.management.FileReadingPositionCache;
import com.aol.advertising.qiao.util.CommonUtils;

/**
 * TextFileTailer reads a file one line at a line (a single ‘line’ of text
 * followed by line feed (‘\n’).
 */
public class TextFileTailer extends AbstractFileTailer<String>
{

    private boolean startFromTailEnd = false;


    /**
     * Creates a Tailer for the given file, with a specified buffer size.
     * 
     * @param file
     *            the file to follow.
     * @param delayMillis
     *            the delay between checks of the file for new content in
     *            milliseconds.
     * @param bufSize
     *            Buffer size
     * @param position
     *            position object
     * @param dataHandler
     *            application-specific data handler
     */
    TextFileTailer(File file, long delayMillis, int bufSize,
            FileReadingPositionCache position,
            ITailerDataHandler<String> dataHandler)
    {
        this(file, delayMillis, bufSize, position, dataHandler, false);

    }


    /**
     * Creates a Tailer for the given file, with a specified buffer size.
     * 
     * @param file
     *            the file to follow.
     * @param delayMillis
     *            the delay between checks of the file for new content in
     *            milliseconds.
     * @param bufSize
     *            Buffer size
     * @param position
     *            position object
     * @param dataHandler
     *            application-specific data handler
     * @param end
     *            Set to true to tail from the end of the file, false to tail
     *            from the beginning of the file.
     */
    TextFileTailer(File file, long delayMillis, int bufSize,
            FileReadingPositionCache position,
            ITailerDataHandler<String> dataHandler, boolean end)
    {
        super(file, delayMillis, bufSize, position, dataHandler);
        this.startFromTailEnd = end;
    }


    /**
     * Creates and starts a Tailer for the given file.
     * 
     * @param file
     *            the file to follow.
     * @param delayMillis
     *            the delay between checks of the file for new content in
     *            milliseconds.
     * @param bufSize
     *            buffer size.
     * @param position
     *            position object
     * @param dataHandler
     *            application-specific data handler
     * @param end
     *            Set to true to tail from the end of the file, false to tail
     *            from the beginning of the file.
     * @return The new tailer
     */
    public static TextFileTailer create(File file, long delayMillis,
            int bufSize, FileReadingPositionCache position,
            ITailerDataHandler<String> dataHandler, boolean end)
    {
        TextFileTailer tailer = new TextFileTailer(file, delayMillis, bufSize,
                position, dataHandler, end);
        return tailer;
    }


    @Override
    public void init() throws Exception
    {
        if (startFromTailEnd)
        {
            this._position.set(tailedFile.length());
            logger.info("start reading from the end of the file...");
        }

        super.init();
    }


    protected boolean process(RandomAccessFile reader, long pullDelayMillis,
            boolean fileRotated) throws InterruptedException
    {
        boolean file_rotation_detected = false;

        try
        {
            long position = this._position.get();
            if (fileRotated)
            {
                _position.set(0);
                position = 0;
                logger.info(">read from offset 0");
            }
            reader.seek(position);

            boolean file_roated = false;
            while (running && !file_rotation_detected)
            {
                // Check the file length to see if it was rotated
                long reader_length = reader.length();

                if (reader_length < position)
                {
                    // file truncated - start from the beginning
                    logFileTruncation(reader_length, position);

                    _position.set(0);
                    position = 0;
                    reader.seek(position);
                    position = readLines(reader);
                    if (logger.isDebugEnabled())
                        logger.debug(">current position: "
                                + _position.toString());
                }
                else if (reader_length > position)
                {
                    // The file has more content than it did last time
                    position = readLines(reader);
                    if (logger.isDebugEnabled())
                        logger.debug(">current position: "
                                + _position.toString());
                }

                boolean has_new_file = hasNewFile(tailedFile,
                        _position.getReadState().checksum);
                if (has_new_file)
                {
                    // try one more time
                    logger.info(">> Seeing a new file.  Checking one more time on existing file to make sure we process everything");
                    position = readLines(reader);
                    logger.info(">> file ending position: "
                            + _position.toString()
                            + ", Going to the new file...");
                    file_roated = true;
                }

                if (file_roated)
                {
                    file_rotation_detected = true;
                }
                else
                {
                    // wait for new content
                    CommonUtils.sleepQuietly(pullDelayMillis); // avoid tight loop   
                }

            }
        }
        catch (InterruptedException e)
        {
            throw e;
        }
        catch (Throwable e)
        {
            if (dataHandler != null)
                dataHandler.onException(e);
        }

        return file_rotation_detected;
    }


    /**
     * Read new lines.
     * 
     * @param reader
     *            The file to read
     * @return The new position after the lines have been read
     * @throws Exception
     */
    private long readLines(RandomAccessFile reader) throws Exception
    {
        safeToShutdown = false;

        try
        {
            StringBuilder sb = new StringBuilder();

            long pos = reader.getFilePointer();
            long rePos = pos; // position to re-read

            int num;
            boolean seenCR = false;
            while (running && ((num = reader.read(inbuf)) != -1))
            {
                for (int i = 0; i < num; i++)
                {
                    byte ch = inbuf[i];

                    switch (ch)
                    {
                        case '\n':
                            seenCR = false; // swallow CR before LF
                            savePositionAndInvokeCallback(sb.toString(),
                                    rePos = pos + i + 1);
                            sb.setLength(0);
                            break;
                        case '\r':
                            if (seenCR)
                                sb.append('\r');
                            seenCR = true;
                            break;
                        default:
                            if (seenCR)
                            {
                                seenCR = false; // swallow final CR
                                savePositionAndInvokeCallback(sb.toString(),
                                        rePos = pos + i + 1);
                                sb.setLength(0);
                            }
                            sb.append((char) ch); // add character, not its ascii value
                    }
                }

                pos = reader.getFilePointer();
            }

            _position.set(rePos);
            return rePos;
        }
        finally
        {
            safeToShutdown = true;
            if (!running)
                throw new InterruptedException("Reader has been interrupted");

        }
    }

}
