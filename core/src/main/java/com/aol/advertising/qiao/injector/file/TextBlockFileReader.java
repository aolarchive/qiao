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

import java.io.RandomAccessFile;
import java.util.List;

/**
 * TextBlockFileReader reads a file one block at a time, until it reaches the
 * end of the file. Data is treated as text string.
 */
public class TextBlockFileReader extends AbstractFileReader<String>
{

    private boolean startFromTailEnd = false;
    private TextBlockSplitter textSplitter = new TextBlockSplitter();


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
    TextBlockFileReader(int bufSize, ITailerDataHandler<String> dataHandler)
    {
        super(bufSize, dataHandler);
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
    public static TextBlockFileReader create(int bufSize,
            ITailerDataHandler<String> dataHandler)
    {
        return new TextBlockFileReader(bufSize, dataHandler);

    }


    @Override
    public void init() throws Exception
    {
        if (startFromTailEnd)
        {
            this.readPosition.set(tailedFile.length());
            logger.info("start reading from the end of the file...");
        }

        super.init();
    }


    protected void process(RandomAccessFile reader) throws InterruptedException
    {

        try
        {
            long position = this.readPosition.get();
            reader.seek(position);

            // Check the file length to see if it was rotated
            long reader_length = reader.length();

            if (reader_length > position)
            {
                // The file has more content than it did last time
                safeToShutdown = false;
                position = readTextBlock(reader);
                if (logger.isDebugEnabled())
                    logger.debug(">current position: "
                            + readPosition.toString());
                safeToShutdown = true; // outside the block

                if (!running)
                    throw new InterruptedException(
                            "Reader has been interrupted");

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

    }


    /**
     * Read a block of texts. A handler separate the block into lines, This will
     * continue to read until end of stream.
     * 
     * @param reader
     *            The file to read
     * @return The new position after a block has been read
     * @throws Exception
     */
    private long readTextBlock(RandomAccessFile reader) throws Exception
    {

        long pos = reader.getFilePointer();
        long rePos = pos; // position to re-read

        int num;
        while (running && ((num = reader.read(inbuf)) != -1))
        {
            savePositionAndInvokeCallback(new String(inbuf, 0, num),
                    rePos = reader.getFilePointer());
        }

        return rePos;

    }


    @Override
    protected int invokeCallback(String data) throws Exception
    {
        List<String> result = textSplitter.splitLines(data);
        try
        {
            for (String line : result)
                super.invokeCallback(line);
            
            return textSplitter.getPositionAdjustment();
        }
        catch (Exception e)
        {
            logger.error("data handler throwed exception: " + e.getMessage(), e);
            throw e;
        }
    }

}
